/*
 *
 *******START PROCEDURE**********************************************************************************************
 *
 */


/*
 *****Step 1**********************************************************************************************************
 *
 * Записуємо дані phone, email, last_name, middle_name, first_name з таблиці dwh.yc_natural_p.
 * Умови перевірки: phone IS NOT NULL, стовпець id в таблиці cs.b_crm_contacts IS NULL
 *
 *****Step 1**********************************************************************************************************
 */

WITH
    filtered_ids AS (
        SELECT DISTINCT
            CASE WHEN array_length(regexp_split_to_array(snp.name, '\s+'), 1) >= 1 THEN initcap((regexp_split_to_array(snp.name, '\s+'))[1]) ELSE NULL END AS last_name,
            CASE WHEN array_length(regexp_split_to_array(snp.name, '\s+'), 1) >= 2 THEN initcap((regexp_split_to_array(snp.name, '\s+'))[2]) ELSE NULL END AS first_name,
            CASE WHEN array_length(regexp_split_to_array(snp.name, '\s+'), 1) >= 3 THEN initcap((regexp_split_to_array(snp.name, '\s+'))[3]) ELSE NULL END AS middle_name,
            CASE WHEN
                (snp.phones::jsonb->>0) IS NOT NULL
                    AND regexp_replace(
                        '+380' || right(regexp_replace((snp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                        '^\+380(.{10})$',
                        '+380\1',
                        'g'
                    ) <> ''
                THEN regexp_replace(
                    '+380' || right(regexp_replace((snp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                    '^\+380(.{10})$',
                    '+380\1',
                    'g'
                )
            END AS phone,
            snp.email
        FROM dwh.yc_natural_p snp
    )
INSERT INTO cs.b_crm_contacts (created_at, updated_at, bank_id, external_id, phone, email, last_name, middle_name, first_name, created_by_id, modify_by_id, project_id)
SELECT DISTINCT ON (fi.phone)
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    fi.phone,
    fi.email,
    fi.last_name,
    fi.middle_name,
    fi.first_name,
    1 AS created_by_id,
    1 AS modify_by_id,
    3 AS project_id
FROM filtered_ids fi
LEFT JOIN cs.b_crm_contacts bctc ON bctc.phone = fi.phone
WHERE 1 = 1
    AND fi.phone IS NOT NULL
    AND bctc.id IS NULL
ORDER BY fi.phone;


/*
 *****Step 2**********************************************************************************************************
 *
 * Із таблиці cs.b_crm_contacts апдейтимо id в таблицю dwh.yc_natural_p в колонку contact_id
 * Умови перевірки: dwh.yc_natural_p.contact_id IS NULL та cs.b_crm_contacts.project_id = 3
 *
 *****Step 2**********************************************************************************************************
 */


WITH
    filter_contact_id AS (
        SELECT DISTINCT
            snp.external_id,
            snp.contact_id,
            CASE WHEN
                (snp.phones::jsonb->>0) IS NOT NULL
                    AND regexp_replace(
                        '+380' || right(regexp_replace((snp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                        '^\+380(.{10})$',
                        '+380\1',
                        'g'
                    ) <> ''
                THEN regexp_replace(
                    '+380' || right(regexp_replace((snp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                    '^\+380(.{10})$',
                    '+380\1',
                    'g'
                )
            END AS phone
        FROM dwh.yc_natural_p snp
        WHERE 1 = 1
            AND snp.contact_id IS NULL
    ),
    update_list AS (
        SELECT DISTINCT
            c.id AS contact_id,
            fi.external_id
            fi.external_id,
            fi.phone,
            fi.contact_id,
            c.id,
            c.phone,
            c2.id
        FROM filter_contact_id fi
        LEFT JOIN cs.b_crm_contacts c ON fi.phone = c.phone
        LEFT JOIN cs.b_crm_contacts c2 ON c2.phone = fi.phone
                                        AND fi.contact_id IS NULL
        WHERE 1 = 1
            AND c.project_id = 3
            AND c2.id IS NULL
    )
SELECT * FROM update_list;
UPDATE dwh.yc_natural_persons SET contact_id = ul.contact_id FROM update_list ul WHERE yc_natural_persons.external_id = ul.external_id;

/*
 *****Step 3**********************************************************************************************************
 *
 * Із таблиці cs.b_crm_contacts c записуємо c.phone, c.email, c.contact_id в таблицю cs.b_crm_t_companies cm
 * Умови перевірки: cs.b_crm_t_companies.contact_id IS NULL та cs.b_crm_contacts.project_id = 3
 *
 *****Step 3**********************************************************************************************************
 */


INSERT INTO cs.b_crm_t_companies (created_at, updated_at, title, phone, email, contact_id, project_id)
SELECT DISTINCT
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    'ФОП' || ' ' || c.last_name || ' ' || c.first_name || ' ' || c.middle_name AS title,
    c.phone,
    c.email,
    c.id AS contact_id,
    3 AS project_id
FROM cs.b_crm_contacts c
LEFT JOIN cs.b_crm_t_companies cm ON cm.contact_id = c.id
WHERE 1 = 1
    AND c.project_id = 3
    AND cm.contact_id IS NULL;


/*
 *****Step 4**********************************************************************************************************
 *
 * Створюємо звязок в таблиці cs.b_crm_contacts_companies між contact_id та company_id
 *
 *****Step 4**********************************************************************************************************
 */


INSERT INTO cs.b_crm_contacts_companies (company_id, contact_id)
SELECT
    c.id AS company_id,
    c.contact_id
FROM cs.b_crm_t_companies c
LEFT JOIN cs.b_crm_contacts_companies cc ON c.id = cc.company_id AND c.contact_id = cc.contact_id
WHERE cc.id IS NULL;

 /*
 ***** Step 5**********************************************************************************************************
 *
 * Із таблиці dwh.yc_natural_p записуємо дані json з рядка economic_activities в стовпці: code, is_main, description company_id таблиці cs.b_crm_t_contact_economic_activities
 * Умови перевірки: cs.b_crm_contacts.id IS NULL та cs.b_crm_t_companies.project_id = 3
 * Через contact_id шукаємо company_id в таблиці cs.b_crm_t_companies
 *
 ***** Step 5**********************************************************************************************************
 */


WITH
    activity_info AS (
        SELECT
            activity ->> 'code' AS activity_code,
            activity ->> 'isMain' AS is_main,
            activity ->> 'description' AS description,
            np.contact_id
        FROM dwh.yc_natural_p np,
             json_array_elements(economic_activities::json) AS activity
    )
INSERT INTO cs.b_crm_t_contact_economic_activities (created_at, updated_at, code, is_main, description, company_id)
SELECT
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    ai.activity_code,
    ai.is_main::bool,
    ai.description,
    cm.id AS company_id
FROM activity_info ai
LEFT JOIN cs.b_crm_t_companies cm ON cm.contact_id = ai.contact_id
LEFT JOIN cs.b_crm_t_contact_economic_activities ea ON ea.code = ai.activity_code AND ea.company_id = cm.id
WHERE cm.project_id = 3
    AND ea.id IS NULL;


/*
 ***** 6 STEP ==============================================================================================================
 *
 * Запис клієнта з легал в контактс
 *
*/


WITH
    parse_signers AS (
        SELECT DISTINCT
            code,
            CASE WHEN
                (slp.phones::jsonb->>0) IS NOT NULL
                    AND regexp_replace(
                        '+380' || right(regexp_replace((slp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                        '^\+380(.{10})$',
                        '+380\1',
                        'g'
                    ) <> ''
                THEN regexp_replace(
                    '+380' || right(regexp_replace((slp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                    '^\+380(.{10})$',
                    '+380\1',
                    'g'
                )
            END AS phone,
            email,
            CASE WHEN array_length(regexp_split_to_array(signer ->> 'name', '\s+'), 1) >= 1 THEN initcap((regexp_split_to_array(signer ->> 'name', '\s+'))[1]) ELSE NULL END AS last_name,
            CASE WHEN array_length(regexp_split_to_array(signer ->> 'name', '\s+'), 1) >= 2 THEN initcap((regexp_split_to_array(signer ->> 'name', '\s+'))[2]) ELSE NULL END AS first_name,
            CASE WHEN array_length(regexp_split_to_array(signer ->> 'name', '\s+'), 1) >= 3 THEN initcap((regexp_split_to_array(signer ->> 'name', '\s+'))[3]) ELSE NULL END AS middle_name,
            LOWER(signer ->> 'role') AS role
        FROM dwh.yc_legal_persons slp,
             json_array_elements(signers::json) AS signer
    )
INSERT INTO cs.b_crm_contacts (created_at, updated_at, bank_id, external_id, phone, email, last_name, first_name, middle_name, created_by_id, modify_by_id, project_id)
SELECT DISTINCT ON (sn.phone)
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    sn.phone,
    sn.email,
    sn.last_name,
    sn.first_name,
    sn.middle_name,
    1 AS created_by_id,
    1 AS modify_by_id,
    3 AS project_id,
    c.id
FROM parse_signers sn
LEFT JOIN cs.b_crm_contacts c ON c.phone = sn.phone AND project_id = 3
WHERE 1 = 1
    AND role LIKE '%керівник%'
    AND c.phone IS NOT NULL
ORDER BY sn.phone;


/*
 ***** Step 7 ============================================================================================================
 *
 * Записуємо дані phone, email, name, code з таблиці dwh.yc_legal_persons.
 * Умови перевірки: phone IS NOT NULL, стовпець id в таблиці cs.b_crm_t_companies IS NULL
 *
 ***** Step 7 ============================================================================================================
 */


WITH
    filtered_company_ids AS (
        SELECT DISTINCT
            slp.code,
            slp.name,
            CASE WHEN
                (slp.phones::jsonb->>0) IS NOT NULL
                    AND regexp_replace(
                        '+380' || right(regexp_replace((slp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                        '^\+380(.{10})$',
                        '+380\1',
                        'g'
                    ) <> ''
                THEN regexp_replace(
                    '+380' || right(regexp_replace((slp.phones::jsonb->>0), '[^+\d]', '', 'g'), 9),
                    '^\+380(.{10})$',
                    '+380\1',
                    'g'
                )
            END AS phone,
            slp.email
        FROM dwh.yc_legal_persons slp
    )
INSERT INTO cs.b_crm_t_companies (created_at, updated_at, title, phone, email, created_by_id, modify_by_id, project_id, ssn)
SELECT DISTINCT ON (fci.phone)
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    name AS title,
    fci.phone,
    fci.email,
    1 AS created_by_id,
    1 AS modify_by_id,
    3 AS project_id,
    code AS ssn
FROM filtered_company_ids fci
LEFT JOIN cs.b_crm_t_companies bctc ON bctc.ssn = fci.code
LEFT JOIN cs.b_crm_contacts c ON c.phone = fci.phone
WHERE 1 = 1
    AND fci.phone IS NOT NULL
    AND bctc.id IS NULL
    AND bctc.phone IS NULL
    AND c.phone IS NOT NULL;


/*
 * Step 8 ============================================================================================================
 *
 * Записуємо дані дані в таблицю t_contact_economic_activities з dwh.yc_legal_persons.
 *
 * Step 8 ============================================================================================================
 */


WITH
    activity_info AS (
        SELECT
            activity ->> 'code' AS activity_code,
            activity ->> 'isMain' AS is_main,
            activity ->> 'description' AS description,
            lp.company_id
        FROM dwh.yc_legal_persons lp,
             json_array_elements(economic_activities::json) AS activity
    )
INSERT INTO cs.b_crm_t_contact_economic_activities (created_at, updated_at, code, is_main, description, company_id)
SELECT
    current_timestamp AS created_at,
    current_timestamp AS updated_at,
    ai.activity_code,
    ai.is_main::bool,
    ai.description,
    cm.id AS company_id
FROM activity_info ai
LEFT JOIN cs.b_crm_t_companies cm ON cm.id = ai.company_id
LEFT JOIN cs.b_crm_t_contact_economic_activities ea ON ea.code = ai.activity_code AND ea.company_id = cm.id
WHERE cm.project_id = 3
    AND ea.id IS NULL;


-- END PROCEDURE

---------------------------------------------------------------------------------------------------------------------

/*
 *******Log finish***************************************************************************************************
 *
 * Процедура закінчила роботу.
 */
 
