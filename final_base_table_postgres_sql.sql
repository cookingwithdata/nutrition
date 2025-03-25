WITH core AS (SELECT *,'uk-source' as data_source FROM public.nutrition_detail_ukgov UNION ALL SELECT *,'usa-source' as data_source FROM public.nutrition_detail_kaggle_usa),

--- Make a list of to recretate dish names, defined as food processes, all the major ones including raw or fresh

processes AS (
  SELECT ARRAY['added','air-popped','baked','batter-dipped','battered','blanched','blend','boiled','braised','breaded','brined','broiled','canned','chilled','chopped','coated','compressed','concentrate','condensed','contains','cooked','covered','crisped','cured','cut','dehydrated','dipped','distilled','drained','dried','dried-frozen','enriched','extract','filled','flavored','freeze-dried','fresh','flesh','fried','frozen','granulated','grilled','ground','heated','homemade','hydrogenated','imitation','in batter','in crumbs','in flour','microwaved','minced','oil-popped','oven-heated','pan-broiled','pan-fried','pasteurized','pre-cooked','preserved','puffed','raw','re-fried','read-to-eat','reconstituted','roasted','salted','seasoned','seeded','steamed','stewed','stewing','stir-fried','stuffed','sweetened','toasted','unsalted','whole','bottled'] as process),

---- First we need to extract the raw ingredients, food processes and or the variety of the raw food ingredient, we factor in differences in how food_name is structured
-- Search the 1st to 10th words in food_name, taking into account commas and whitespace, for all of the processes
all_processes as (
  SELECT
  food_name,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 1) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 1)
    WHEN SPLIT_PART(food_name, ', ', 1) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 1)
  END AS first_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 2) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 2)
    WHEN SPLIT_PART(food_name, ', ', 2) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 2)
  END AS second_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 3) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 3)
    WHEN SPLIT_PART(food_name, ', ', 3) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 3)
  END AS third_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 4) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 4)
    WHEN SPLIT_PART(food_name, ', ', 4) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 4)
  END AS fourth_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 5) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 5)
    WHEN SPLIT_PART(food_name, ', ', 5) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 5)
  END AS fifth_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 6) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 6)
    WHEN SPLIT_PART(food_name, ', ', 6) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 6)
  END AS sixth_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 7) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 7)
    WHEN SPLIT_PART(food_name, ', ', 7) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 7)
  END AS seventh_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 8) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 8)
    WHEN SPLIT_PART(food_name, ', ', 8) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 8)
  END AS eigth_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 9) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 9)
    WHEN SPLIT_PART(food_name, ', ', 9) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 9)
  END AS ninth_word,
  CASE 
    WHEN SPLIT_PART(food_name, ' ', 10) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ' ', 10)
    WHEN SPLIT_PART(food_name, ', ', 10) = ANY(ARRAY(SELECT UNNEST(process) FROM processes)) THEN SPLIT_PART(food_name, ', ', 10)
  END AS tenth_word
  FROM core
),

--- now create a middle table, which will be cleaned columns and create the 'process' column for the uk only
  all_middle_table as (
SELECT 
DISTINCT
  CAST(d.food_name AS varchar) as food_name,
  CAST(d.group_code AS varchar) as group_code,
  CAST(d.group_name AS varchar) as group_name,
  CAST(d.data_source AS varchar) as data_source,
  CAST(ARRAY_TO_STRING(ARRAY[p.first_word,p.second_word,p.third_word,p.fourth_word,p.fifth_word,p.sixth_word,p.seventh_word,p.eigth_word, p.ninth_word, p.tenth_word],',') as varchar) as process,
  CAST(d.serving_size AS varchar) as serving_size,
  CAST(d.kcal as NUMERIC) as kcal,
  CAST(d.water_g as NUMERIC) as water_g,
  CAST(d.fiber_g as NUMERIC) as fiber_g,
  CAST((d.sodium_mg/10) as NUMERIC) as sodium_g,
  CAST(d.total_fat_g AS NUMERIC) as total_fat_g,
  CAST(d.saturated_fat_g AS NUMERIC) as saturated_fat_g,
  CAST(d.monounsaturated_fatty_acids_g AS NUMERIC) as monounsaturated_fat_g,
  CAST(d.polyunsaturated_fatty_acids_g AS NUMERIC) as polyunsaturated_fat_g,
  CAST(d.trans_fat_g AS NUMERIC) as trans_fat_g,
  CAST(d.cholesterol_mg AS NUMERIC) as cholesterol_mg,
  CAST(d.protein_g AS NUMERIC) as protein_g,
  CAST(d.carbohydrate_g AS NUMERIC) as carbohydrate_g,
  CAST(d.total_sugars_g AS NUMERIC) as total_sugars_g,
  CAST(d.glucose_g AS NUMERIC) as glucose_g,
  CAST(d.galactose_g AS NUMERIC) as galactose_g,
  CAST(d.fructose_g AS NUMERIC) as fructose_g,
  CAST(d.sucrose_g AS NUMERIC) as sucrose_g,
  CAST(d.maltose_g AS NUMERIC) as maltose_g,
  CAST(d.lactose_g AS NUMERIC) as lactose_g,
  --(CAST(REPLACE(d.vitamin_a_iu, ' mcg', '') as NUMERIC) * 0.025) -(d.carotene_beta_mcg/6) as retinol_mcg,
  CAST(d.retinol_mcg AS NUMERIC) as retinol_mcg,
  CAST(d.carotene_alpha_mcg AS NUMERIC) as carotene_alpha_mcg,
  CAST(d.carotene_beta_mcg AS NUMERIC) as carotene_beta_mcg,
  CAST(d.vitamin_a_iu AS NUMERIC) as vitamin_a_iu,
  --CAST(REPLACE(d.vitamin_a_iu, ' mcg', '') as NUMERIC) * 0.025 as vitamin_a_mcg,
  CAST(d.vitamin_a_mcg AS NUMERIC) as vitamin_a_mcg,
  CAST(d.thiamin_mg AS NUMERIC) as thiamin_mg,
  CAST(d.riboflavin_mg AS NUMERIC) as riboflavin_mg,
  CAST(NULL AS NUMERIC) as niacin_mg,
  CAST(d.choline_mg AS NUMERIC) as choline_mg,
  CAST(d.pantothenic_acid_mg as NUMERIC) * 1.087 as pantothenate_mg,
  CAST(d.pantothenic_acid_mg AS NUMERIC) as pantothenic_acid_mg,
  CAST(d.vitamin_b6_mg AS NUMERIC) as vitamin_b6_mg,
  CAST(NULL AS NUMERIC) as biotin_mcg,
  CAST(d.folate_mcg AS NUMERIC) as folate_mcg,
  CAST(d.vitamin_b12_mcg AS NUMERIC) as vitamin_b12_mcg,
  CAST(d.vitamin_c_mg AS NUMERIC) as vitamin_c_mg,
  CAST(d.vitamin_d_iu AS NUMERIC) as vitamin_d_iu,
  CAST(d.vitamin_d_iu as NUMERIC) * 0.025 as vitamin_d_mcg,
  CAST(d.vitamin_e_mg AS NUMERIC) as vitamin_e_mg,
  CAST(d.vitamin_k_mcg AS NUMERIC) as vitamin_k_mcg,
  CAST(d.tryptophan_g AS NUMERIC) as tryptophan_g,
  CAST(d.lutein_mcg AS NUMERIC) as lutein_mcg,
  CAST(d.potassium_mg AS NUMERIC) as potassium_mg,
  CAST(d.calcium_mg AS NUMERIC) as calcium_mg,
  CAST(d.magnesium_mg AS NUMERIC) as magnesium_mg,
  CAST(d.phosphorous_mg AS NUMERIC) as phosphorous_mg,
  CAST(d.iron_mg AS NUMERIC) as iron_mg,
  CAST(d.copper_mg AS NUMERIC) as copper_mg,
  CAST(d.zinc_mg AS NUMERIC) as zinc_mg,
  CAST(d.chloride_mg AS NUMERIC) as chloride_mg,
  CAST(d.manganese_mg AS NUMERIC) as manganese_mg,
  CAST(d.selenium_mcg AS NUMERIC) as selenium_mcg,
  CAST(d.iodine_mcg AS NUMERIC) as iodine_mcg
FROM core as d
LEFT JOIN all_processes as p
  ON d.food_name = p.food_name
),

--- in order to find the emissions for each raw ingredient and cleaned median for uk data where needed (for instance where the value is 99 or 0.00001) we need to extract the raw ingredient from the food name to be able to join to the emissisons table, it will also be used for food groups and getting emissions that way if the necessity occurs
--- we also pull the main_ingredient_process (raw or not) as well as the fimaly ingredient or process dependding from the food_name
ingredients_extraction as (
SELECT 
  DISTINCT
  food_name,
  group_code,
  group_name,
  -- Check for the first comma and extract the word before it
  CASE 
    WHEN POSITION(',' IN food_name) > 0 THEN 
      SUBSTRING(food_name FROM 1 FOR POSITION(',' IN food_name) - 1)
    ELSE 
      food_name
  END AS raw_ingredient,
  -- Check for a second comma; if present, extract the word after the first comma up to the second comma.
  -- If no second comma, take all words after the first comma
  CASE 
    WHEN POSITION(',' IN SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1)) > 0 THEN 
      SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1 
                FOR POSITION(',' IN SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1)) - 1)
    ELSE 
      TRIM(SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1))
  END AS main_ingredient_process,
  -- Check if a second comma exists; if so, get the word after the second comma.
  -- If no second comma, return an empty string
  CASE 
    WHEN POSITION(',' IN SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1)) > 0 THEN 
      TRIM(SUBSTRING(food_name FROM POSITION(',' IN SUBSTRING(food_name FROM POSITION(',' IN food_name) + 1)) 
                     + POSITION(',' IN food_name) + 2))
    ELSE 
      '' 
  END AS family_ingredient_or_process,
  process
FROM all_middle_table
),

raw_ingredients AS (
SELECT 
  DISTINCT
  f.food_name,
  -- Check if there is a space and extract text before the first space; if no space, return the whole string
  CASE 
    WHEN POSITION(' ' IN f.raw_ingredient) > 0 THEN 
      SUBSTRING(f.raw_ingredient FROM 1 FOR POSITION(' ' IN f.raw_ingredient) - 1)
    ELSE 
      f.raw_ingredient
  END AS raw_ingredient,
  -- Check if there is a second space, and extract text between the first and second spaces;
  -- if no second space, extract from the first space to the end
  CASE 
    WHEN POSITION(' ' IN SUBSTRING(f.raw_ingredient FROM POSITION(' ' IN f.raw_ingredient) + 1)) > 0 THEN 
      SUBSTRING(f.raw_ingredient 
                FROM POSITION(' ' IN f.raw_ingredient) + 1 
                FOR POSITION(' ' IN SUBSTRING(f.raw_ingredient FROM POSITION(' ' IN f.raw_ingredient) + 1)) - 1)
    ELSE 
      TRIM(SUBSTRING(f.raw_ingredient FROM POSITION(' ' IN f.raw_ingredient) + 1))
  END AS dish_name,
  f.process AS dish_process,
  TRIM(f.main_ingredient_process) AS main_ingredient_process,
  TRIM(f.family_ingredient_or_process) AS family_ingredient_or_final_process,
  f.group_code,
  f.group_name
FROM ingredients_extraction AS f
),

grab_ingredients as (
SELECT
  a.food_name,
  e.raw_ingredient,
  e.dish_name,
  e.dish_process,
  e.main_ingredient_process,
  e.family_ingredient_or_final_process,
  a.group_code,
  a.group_name,
  a.data_source,
  a.process,
  a.serving_size,
  a.kcal,
  a.water_g,
  a.fiber_g,
  a.sodium_g,
  a.total_fat_g,
  a.saturated_fat_g,
  a.monounsaturated_fat_g,
  a.polyunsaturated_fat_g,
  a.trans_fat_g,
  a.cholesterol_mg,
  a.protein_g,
  a.carbohydrate_g,
  a.total_sugars_g,
  a.glucose_g,
  a.galactose_g,
  a.fructose_g,
  a.sucrose_g,
  a.maltose_g,
  a.lactose_g,
  a.retinol_mcg,
  a.carotene_alpha_mcg,
  a.carotene_beta_mcg,
  a.vitamin_a_iu,
  a.vitamin_a_mcg,
  a.thiamin_mg,
  a.riboflavin_mg,
  a.niacin_mg,
  a.choline_mg,
  a.pantothenate_mg,
  a.pantothenic_acid_mg,
  a.vitamin_b6_mg,
  a.biotin_mcg,
  a.folate_mcg,
  a.vitamin_b12_mcg,
  a.vitamin_c_mg,
  a.vitamin_d_iu,
  a.vitamin_d_mcg,
  a.vitamin_e_mg,
  a.vitamin_k_mcg,
  a.tryptophan_g,
  a.lutein_mcg,
  a.potassium_mg,
  a.calcium_mg,
  a.magnesium_mg,
  a.phosphorous_mg,
  a.iron_mg,
  a.copper_mg,
  a.zinc_mg,
  a.chloride_mg,
  a.manganese_mg,
  a.selenium_mcg,
  a.iodine_mcg
FROM all_middle_table as a
INNER JOIN raw_ingredients as e
	ON a.food_name = e.food_name
),
-- now using the combined dataset we are now filling in data for the various nutrients and vitamins in the UK dataset, where there are N or Tr equivalent values ('99', and '0.0001' respectively) present.
--- if these are present we use the data from the usa data and then get the median
uk_values_cleaned as (
SELECT 
u.food_name,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.kcal in (99,0.0001,0) OR  u.kcal is null THEN f.kcal ELSE u.kcal END)) as kcal,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.water_g in (99,0.0001,0) OR  u.water_g is null THEN f.water_g ELSE u.water_g END)) as water_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.fiber_g in (99,0.0001,0) OR  u.fiber_g is null THEN f.fiber_g ELSE u.fiber_g END)) as fiber_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.sodium_g in (99,0.0001,0) OR  u.sodium_g is null THEN f.sodium_g ELSE u.sodium_g  END)) as sodium_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.total_fat_g in (99,0.0001,0) OR  u.total_fat_g is null THEN f.total_fat_g  ELSE u.total_fat_g  END)) as total_fat_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.saturated_fat_g in (99,0.0001,0) OR  u.saturated_fat_g is null THEN f.saturated_fat_g  ELSE u.saturated_fat_g  END)) as saturated_fat_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.monounsaturated_fat_g in (99,0.0001,0) OR  u.monounsaturated_fat_g is null THEN f.monounsaturated_fat_g  ELSE u.monounsaturated_fat_g  END)) as monounsaturated_fat_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.polyunsaturated_fat_g in (99,0.0001,0) OR  u.polyunsaturated_fat_g is null THEN f.polyunsaturated_fat_g  ELSE u.polyunsaturated_fat_g  END)) as polyunsaturated_fat_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.trans_fat_g in (99,0.0001,0) OR  u.trans_fat_g is null THEN f.trans_fat_g  ELSE u.trans_fat_g  END)) as trans_fat_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.cholesterol_mg in (99,0.0001,0) OR  u.cholesterol_mg is null THEN f.cholesterol_mg  ELSE u.cholesterol_mg  END)) as cholesterol_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.protein_g in (99,0.0001,0) OR  u.protein_g is null THEN f.protein_g  ELSE u.protein_g  END)) as protein_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.carbohydrate_g in (99,0.0001,0) OR  u.carbohydrate_g is null THEN f.carbohydrate_g  ELSE u.carbohydrate_g  END)) as carbohydrate_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.total_sugars_g in (99,0.0001,0) OR  u.total_sugars_g is null THEN f.total_sugars_g  ELSE u.total_sugars_g  END)) as total_sugars_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.glucose_g in (99,0.0001,0) OR u.glucose_g is null THEN f.glucose_g  ELSE u.glucose_g  END)) as glucose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.galactose_g in (99,0.0001,0) OR u.galactose_g is null THEN f.galactose_g  ELSE u.galactose_g  END)) as galactose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.fructose_g in (99,0.0001,0) OR u.fructose_g is null THEN f.fructose_g  ELSE u.fructose_g  END)) as fructose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.sucrose_g in (99,0.0001,0) OR u.sucrose_g is null THEN f.sucrose_g  ELSE u.sucrose_g  END)) as sucrose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.maltose_g in (99,0.0001,0) OR u.maltose_g is null THEN f.maltose_g  ELSE u.maltose_g  END)) as maltose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.lactose_g in (99,0.0001,0) OR u.lactose_g is null THEN f.lactose_g  ELSE u.lactose_g  END)) as lactose_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.retinol_mcg in (99,0.0001,0) OR u.retinol_mcg is null THEN f.retinol_mcg  ELSE u.retinol_mcg  END)) as retinol_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.carotene_alpha_mcg in (99,0.0001,0) OR u.carotene_alpha_mcg is null THEN f.carotene_alpha_mcg  ELSE u.carotene_alpha_mcg  END)) as carotene_alpha_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.carotene_beta_mcg in (99,0.0001,0) OR u.carotene_beta_mcg is null THEN f.carotene_beta_mcg  ELSE u.carotene_beta_mcg  END)) as carotene_beta_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_a_iu in (99,0.0001,0) OR u.vitamin_a_iu is null THEN f.vitamin_a_iu  ELSE u.vitamin_a_iu  END)) as vitamin_a_iu,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_a_mcg in (99,0.0001,0) OR u.vitamin_a_mcg is null THEN f.vitamin_a_mcg  ELSE u.vitamin_a_mcg  END)) as vitamin_a_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.thiamin_mg in (99,0.0001,0) OR u.thiamin_mg is null THEN f.thiamin_mg  ELSE u.thiamin_mg  END)) as thiamin_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.riboflavin_mg in (99,0.0001,0) OR u.riboflavin_mg is null THEN f.riboflavin_mg  ELSE u.riboflavin_mg  END)) as riboflavin_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.niacin_mg in (99,0.0001,0) OR u.niacin_mg is null THEN f.niacin_mg  ELSE u.niacin_mg  END)) as niacin_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.choline_mg in (99,0.0001,0) OR u.choline_mg is null THEN f.choline_mg  ELSE u.choline_mg  END)) as choline_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.pantothenate_mg in (99,0.0001,0) OR u.pantothenate_mg is null THEN f.pantothenate_mg  ELSE u.pantothenate_mg  END)) as pantothenate_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.pantothenic_acid_mg in (99,0.0001,0) OR u.pantothenic_acid_mg is null THEN f.pantothenic_acid_mg  ELSE u.pantothenic_acid_mg  END)) as pantothenic_acid_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_b6_mg in (99,0.0001,0) OR u.vitamin_b6_mg is null THEN f.vitamin_b6_mg  ELSE u.vitamin_b6_mg  END)) as vitamin_b6_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.biotin_mcg in (99,0.0001,0) OR u.biotin_mcg is null THEN f.biotin_mcg  ELSE u.biotin_mcg  END)) as biotin_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.folate_mcg in (99,0.0001,0) OR u.folate_mcg is null THEN f.folate_mcg  ELSE u.folate_mcg  END)) as folate_mcg ,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_b12_mcg in (99,0.0001,0) OR u.vitamin_b12_mcg is null THEN f.vitamin_b12_mcg  ELSE u.vitamin_b12_mcg  END)) as vitamin_b12_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_c_mg in (99,0.0001,0) OR u.vitamin_c_mg is null THEN f.vitamin_c_mg  ELSE u.vitamin_c_mg  END)) as vitamin_c_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_d_iu in (99,0.0001,0) OR u.vitamin_d_iu is null THEN f.vitamin_d_iu  ELSE u.vitamin_d_iu  END)) as vitamin_d_iu,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_d_mcg in (99,0.0001,0) OR u.vitamin_d_mcg is null THEN f.vitamin_d_mcg  ELSE u.vitamin_d_mcg  END)) as vitamin_d_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_e_mg in (99,0.0001,0) OR u.vitamin_e_mg is null THEN f.vitamin_e_mg  ELSE u.vitamin_e_mg  END)) as vitamin_e_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.vitamin_k_mcg in (99,0.0001,0) OR u.vitamin_k_mcg is null THEN f.vitamin_k_mcg  ELSE u.vitamin_k_mcg  END)) as vitamin_k_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.tryptophan_g in (99,0.0001,0) OR u.tryptophan_g is null THEN f.tryptophan_g  ELSE u.tryptophan_g  END)) as tryptophan_g,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.lutein_mcg in (99,0.0001,0) OR u.lutein_mcg is null THEN f.lutein_mcg  ELSE u.lutein_mcg  END)) as lutein_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.potassium_mg in (99,0.0001,0) OR u.potassium_mg is null THEN f.potassium_mg  ELSE u.potassium_mg  END)) as potassium_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.calcium_mg in (99,0.0001,0) OR u.calcium_mg is null THEN f.calcium_mg  ELSE u.calcium_mg  END)) as calcium_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.magnesium_mg in (99,0.0001,0) OR u.magnesium_mg is null THEN f.magnesium_mg  ELSE u.magnesium_mg  END)) as magnesium_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.phosphorous_mg in (99,0.0001,0) OR u.phosphorous_mg is null THEN f.phosphorous_mg  ELSE u.phosphorous_mg  END)) as phosphorous_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.iron_mg in (99,0.0001,0) OR u.iron_mg is null THEN f.iron_mg  ELSE u.iron_mg  END)) as iron_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.copper_mg in (99,0.0001,0) OR u.copper_mg is null THEN f.copper_mg  ELSE u.copper_mg  END)) as copper_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.zinc_mg in (99,0.0001,0) OR u.zinc_mg  is null THEN f.zinc_mg  ELSE u.zinc_mg  END)) as zinc_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.chloride_mg in (99,0.0001,0) OR u.chloride_mg is null THEN f.chloride_mg  ELSE u.chloride_mg  END)) as chloride_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.manganese_mg in (99,0.0001,0) OR u.manganese_mg is null THEN f.manganese_mg  ELSE u.manganese_mg  END)) as manganese_mg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.selenium_mcg in (99,0.0001,0) OR u.selenium_mcg is null THEN f.selenium_mcg  ELSE u.selenium_mcg  END)) as selenium_mcg,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN u.iodine_mcg in (99,0.0001,0) OR u.iodine_mcg is null OR u.iodine_mcg=0 THEN f.iodine_mcg  ELSE u.iodine_mcg  END)) as iodine_mcg
FROM grab_ingredients as u
INNER JOIN grab_ingredients as f 
	ON u.raw_ingredient = f.raw_ingredient 
	AND f.data_source = 'usa-source'
WHERE u.data_source = 'uk-source'
GROUP BY 1
),

---we recreate the final  table from the newly transformed data
all_final_middle_table AS (
select
  u.food_name,
  u.raw_ingredient,
  u.dish_name,
  u.dish_process,
  u.main_ingredient_process,
  u.family_ingredient_or_final_process,
  u.group_code,
  u.group_name,
  u.data_source,
  u.process,
  u.serving_size,
  COALESCE(uc.kcal, u.kcal,0) as kcal,
  COALESCE(uc.water_g, u.water_g,0) as water_g,
  COALESCE(uc.fiber_g, u.fiber_g,0) as fiber_g,
  COALESCE(uc.sodium_g, u.sodium_g,0) as sodium_g,
  COALESCE(uc.total_fat_g, u.total_fat_g,0) as total_fat_g,
  COALESCE(uc.saturated_fat_g, u.saturated_fat_g,0) as saturated_fat_g,
  COALESCE(uc.monounsaturated_fat_g, u.monounsaturated_fat_g,0) as monounsaturated_fat_g,
  COALESCE(uc.polyunsaturated_fat_g, u.polyunsaturated_fat_g,0) as polyunsaturated_fat_g,
  COALESCE(uc.trans_fat_g, u.trans_fat_g,0) as trans_fat_g,
  COALESCE(uc.cholesterol_mg, u.cholesterol_mg,0) as cholesterol_mg,
  COALESCE(uc.protein_g, u.protein_g,0) as protein_g,
  COALESCE(uc.carbohydrate_g, u.carbohydrate_g,0) as carbohydrate_g,
  COALESCE(uc.total_sugars_g, u.total_sugars_g,0) as total_sugars_g,
  COALESCE(uc.glucose_g, u.glucose_g,0) as glucose_g,
  COALESCE(uc.galactose_g, u.galactose_g,0) as galactose_g,
  COALESCE(uc.fructose_g, u.fructose_g,0) as fructose_g,
  COALESCE(uc.sucrose_g, u.sucrose_g,0) as sucrose_g,
  COALESCE(uc.maltose_g, u.maltose_g,0) as maltose_g,
  COALESCE(uc.lactose_g, u.lactose_g,0) as lactose_g,
  COALESCE(uc.retinol_mcg, u.retinol_mcg,0) as retinol_mcg,
  COALESCE(uc.carotene_alpha_mcg, u.carotene_alpha_mcg,0) as carotene_alpha_mcg,
  COALESCE(uc.carotene_beta_mcg, u.carotene_beta_mcg,0) as carotene_beta_mcg,
  COALESCE(uc.vitamin_a_iu, u.vitamin_a_iu,0) as vitamin_a_iu,
  COALESCE(uc.vitamin_a_mcg, u.vitamin_a_mcg,0) as vitamin_a_mcg,
  COALESCE(uc.thiamin_mg, u.thiamin_mg,0) as thiamin_mg,
  COALESCE(uc.riboflavin_mg, u.riboflavin_mg,0) as riboflavin_mg,
  COALESCE(uc.niacin_mg, u.niacin_mg,0) as niacin_mg,
  COALESCE(uc.choline_mg, u.choline_mg,0) as choline_mg,
  COALESCE(uc.pantothenate_mg, u.pantothenate_mg,0) as pantothenate_mg,
  COALESCE(uc.pantothenic_acid_mg, u.pantothenic_acid_mg,0) as pantothenic_acid_mg,
  COALESCE(uc.vitamin_b6_mg, u.vitamin_b6_mg,0) as vitamin_b6_mg,
  COALESCE(uc.biotin_mcg, u.biotin_mcg,0) as biotin_mcg,
  COALESCE(uc.folate_mcg, u.folate_mcg,0) as folate_mcg,
  COALESCE(uc.vitamin_b12_mcg, u.vitamin_b12_mcg,0) as vitamin_b12_mcg,
  COALESCE(uc.vitamin_c_mg, u.vitamin_c_mg,0) as vitamin_c_mg,
  COALESCE(uc.vitamin_d_iu, u.vitamin_d_iu,0) as vitamin_d_iu,
  COALESCE(uc.vitamin_d_mcg, u.vitamin_d_mcg,0) as vitamin_d_mcg,
  COALESCE(uc.vitamin_e_mg, u.vitamin_e_mg,0) as vitamin_e_mg,
  COALESCE(uc.vitamin_k_mcg, u.vitamin_k_mcg,0) as vitamin_k_mcg,
  COALESCE(uc.tryptophan_g, u.tryptophan_g,0) as tryptophan_g,
  COALESCE(uc.lutein_mcg, u.lutein_mcg,0) as lutein_mcg,
  COALESCE(uc.potassium_mg, u.potassium_mg,0) as potassium_mg,
  COALESCE(uc.calcium_mg, uc.calcium_mg,0) as calcium_mg,
  COALESCE(uc.magnesium_mg, u.magnesium_mg,0) as magnesium_mg,
  COALESCE(uc.phosphorous_mg, u.phosphorous_mg,0) as phosphorous_mg,
  COALESCE(uc.iron_mg, u.iron_mg,0) as iron_mg,
  COALESCE(uc.copper_mg, u.copper_mg,0) as copper_mg,
  COALESCE(uc.zinc_mg, u.zinc_mg,0) as zinc_mg,
  COALESCE(uc.chloride_mg, u.chloride_mg,0) as chloride_mg,
  COALESCE(uc.manganese_mg, u.manganese_mg,0) as manganese_mg,
  COALESCE(uc.selenium_mcg, u.selenium_mcg,0) as selenium_mcg,
  COALESCE(uc.iodine_mcg, u.iodine_mcg,0) as iodine_mcg
 FROM grab_ingredients as u 
 LEFT JOIN uk_values_cleaned as uc ON u.food_name = uc.food_name AND u.data_source = 'uk-source'
),
--- Now since some food_names are duplicated across uk and usa datasets, but may have varrying values of nutrients then we need to get the averages in order to be used to calculate the emissions that each food groupo emits
 averages_table AS (
SELECT 
  food_name,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY kcal) as kcal,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY water_g) as water_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fiber_g) as fiber_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sodium_g) as sodium_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_fat_g) as total_fat_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY saturated_fat_g) as saturated_fat_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY monounsaturated_fat_g) as monounsaturated_fat_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY polyunsaturated_fat_g) as polyunsaturated_fat_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY trans_fat_g) as trans_fat_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cholesterol_mg) as cholesterol_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY protein_g) as protein_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY carbohydrate_g) as carbohydrate_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_sugars_g) as total_sugars_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY glucose_g) as glucose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY galactose_g) as galactose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY fructose_g) as fructose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY sucrose_g) as sucrose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY maltose_g) as maltose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lactose_g) as lactose_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY retinol_mcg) as retinol_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY carotene_alpha_mcg) as carotene_alpha_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY carotene_beta_mcg) as carotene_beta_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_a_iu) as vitamin_a_iu,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_a_mcg) as vitamin_a_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY thiamin_mg) as thiamin_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY riboflavin_mg) as riboflavin_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY niacin_mg) as niacin_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY choline_mg) as choline_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pantothenate_mg) as pantothenate_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY pantothenic_acid_mg) as pantothenic_acid_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_b6_mg) as vitamin_b6_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY biotin_mcg) as biotin_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY folate_mcg) as folate_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_b12_mcg) as vitamin_b12_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_c_mg) as vitamin_c_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_d_iu) as vitamin_d_iu,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_d_mcg) as vitamin_d_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_e_mg) as vitamin_e_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY vitamin_k_mcg) as vitamin_k_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tryptophan_g) as tryptophan_g,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lutein_mcg) as lutein_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY potassium_mg) as potassium_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY calcium_mg) as calcium_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY magnesium_mg) as magnesium_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY phosphorous_mg) as phosphorous_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY iron_mg) as iron_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY copper_mg) as copper_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY zinc_mg) as zinc_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY chloride_mg) as chloride_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY manganese_mg) as manganese_mg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY selenium_mcg) as selenium_mcg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY iodine_mcg) as iodine_mcg
FROM all_final_middle_table
GROUP BY 1
),

--- pull raw ingredients emissions data
 emissions  AS (SELECT * FROM public.raw_foods_global_emmissions_kgCO2_perkg),

--- pull detail grouped food emissions data 
 detail_emissions as (
SELECT
  group_code,
  group_name,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_land_use_kgCo2_perkg) as food_emissions_land_use_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_farm_kgCo2_perkg) as food_emissions_farm_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_animal_feed_kgCo2_perkg) as food_emissions_animal_feed_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_processing_kgCo2_perkg) as food_emissions_processing_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_transport_kgCo2_perkg) as food_emissions_transport_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_retail_kgCo2_perkg) as food_emissions_retail_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_packaging_kgCo2_perkg) as food_emissions_packaging_kgCo2_perk,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY food_emissions_losses_kgCo2_perkg) as food_emissions_losses_kgCo2_perkg,
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY total_emissions) as total_emissions
FROM public.detail_emmissions_food_grouped
WHERE group_name is not null
GROUP BY 1,2
ORDER BY 1),

--- get the emissions using raw_ingredient, pull the protein, fat and kcal to use later on
food_emiss as (
SELECT 
	DISTINCT
	r.food_name,
	r.raw_ingredient,
	r.group_code,
	r.group_name,
	m.kcal,
	m.protein_g,
	m.total_fat_g,
	e.emissions_pkg/10 as emissions_pserving,
	e.emissions_pkg,
	e.emissions_p1kcalories,
	e.emissions_p100gprotein,
	e.emissions_p100gfat
FROM raw_ingredients as r
LEFT JOIN averages_table as m
	on r.food_name = m.food_name
LEFT JOIN emissions as e
	ON lower(r.raw_ingredient) = lower(e.raw_food) 
),

--- get the emissions using the grouped emissions table, since this has a wider variety of groups - join these onto food_emiss for where an emission doesn't already join using raw_ingredient.
--- (for about 10% of results) calculate emissions by calories, protein and fat, since there are no values in the raw ingredients emissions table for some of these rows, we have to caluclate them
food_no_emiss as (
SELECT
	DISTINCT
	lj.food_name,
	lj.raw_ingredient,
	lj.group_code,
	lj.group_name,
	lj.kcal,
	ee.total_emissions/10 as emissions_pserving,
	ee.total_emissions as emissions_pkg,
	CASE WHEN lj.kcal=0 THEN 0 ELSE (ee.total_emissions/10) / (lj.kcal/1000) END as emissions_p1kcalories,
	CASE WHEN lj.protein_g=0 THEN 0 ELSE (ee.total_emissions/10) / (lj.protein_g/100) END as emissions_p100gprotein,
	CASE WHEN lj.total_fat_g=0 THEN 0 ELSE (ee.total_emissions/10) / (lj.total_fat_g/100) END as emissions_p100gfat
FROM food_emiss as lj
LEFT JOIN detail_emissions as ee
	ON lj.group_code = ee.group_code
WHERE lj.emissions_pkg is null and lj.group_code is not null
),

--- combine previous CTEs and default to 0 if no emissions found
combo_emiss as (
SELECT 
	l1.food_name,
	l1.raw_ingredient,
	l1.group_code,
    COALESCE(l1.emissions_pserving,l2.emissions_pserving,0) as emissions_pserving,
	COALESCE(l1.emissions_pkg,l2.emissions_pkg,0) as emissions_pkg,
	COALESCE(l1.emissions_p1kcalories,l2.emissions_p1kcalories,0) as emissions_p1kcalories,
	COALESCE(l1.emissions_p100gprotein,l2.emissions_p100gprotein,0) as emissions_p100gprotein,
	COALESCE(l1.emissions_p100gfat,l2.emissions_p100gfat,0) as emissions_p100gfat
FROM food_emiss  as l1
LEFT JOIN food_no_emiss as l2
	ON l1.food_name = l2.food_name 
),

--- get the median emissions by food group for the results from combo_emiss that still don't have emission data
food_no_grouped_emiss as (
SELECT
	n.food_name,
	n.raw_ingredient,
	n.group_code,
	(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.emissions_pkg))/10 as emissions_pserving,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.emissions_pkg) as emissions_pkg,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.emissions_p1kcalories) as emissions_p1kcalories,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.emissions_p100gprotein) as emissions_p100gprotein,
	PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY e.emissions_p100gfat) as emissions_p100gfat
FROM combo_emiss as n
LEFT JOIN emissions as e
	ON n.group_code = e.group_code
where n.emissions_pkg = 0
GROUP BY 1,2,3
),
--- combine all emissions data together
final_combo_emiss as (
SELECT 
	c.food_name,
	c.raw_ingredient,
	c.group_code,
	COALESCE(f.emissions_pserving,c.emissions_pserving,0) as emissions_pserving,
  COALESCE(f.emissions_pkg,c.emissions_pkg,0) as emissions_pkg,
	COALESCE(f.emissions_p1kcalories,c.emissions_p1kcalories,0) as emissions_p1kcalories,
	COALESCE(f.emissions_p100gprotein,c.emissions_p100gprotein,0) as emissions_p100gprotein,
	COALESCE(f.emissions_p100gfat,c.emissions_p100gfat,0) as emissions_p100gfat
FROM combo_emiss as c
LEFT JOIN food_no_grouped_emiss as f
	ON c.food_name = f.food_name
),
 final_table as (
select 
distinct
	b.raw_ingredient,
	b.food_name,
	b.dish_process,
	b.main_ingredient_process,
	b.dish_name,
	b.family_ingredient_or_final_process,
	b.group_code,
	b.group_name,
	b.data_source,
	e.emissions_pserving as emissions_serving_size,
	e.emissions_p1kcalories as emissions_calories_content,
	e.emissions_p100gprotein as emissions_protein_content,
	e.emissions_p100gfat as emissions_fat_content,
	a.kcal,
	a.water_g,
	a.fiber_g,
	a.sodium_g,
	a.total_fat_g,
	a.saturated_fat_g,
	a.monounsaturated_fat_g,
	a.polyunsaturated_fat_g,
	a.trans_fat_g,
	a.cholesterol_mg,
	a.protein_g,
	a.carbohydrate_g,
	a.total_sugars_g,
	a.glucose_g,
	a.galactose_g,
	a.fructose_g,
	a.sucrose_g,
	a.maltose_g,
	a.lactose_g,
	a.retinol_mcg,
	a.carotene_alpha_mcg,
	a.carotene_beta_mcg,
	a.vitamin_a_iu,
	a.vitamin_a_mcg,
	a.thiamin_mg,
	a.riboflavin_mg,
	a.niacin_mg,
	a.choline_mg,
	a.pantothenate_mg,
	a.pantothenic_acid_mg,
	a.vitamin_b6_mg,
	a.biotin_mcg,
	a.folate_mcg,
	a.vitamin_b12_mcg,
	a.vitamin_c_mg,
	a.vitamin_d_iu,
	a.vitamin_d_mcg,
	a.vitamin_e_mg,
	a.vitamin_k_mcg,
	a.tryptophan_g,
	a.lutein_mcg,
	a.potassium_mg,
	a.calcium_mg,
	a.magnesium_mg,
	a.phosphorous_mg,
	a.iron_mg,
	a.copper_mg,
	a.zinc_mg,
	a.chloride_mg,
	a.manganese_mg,
	a.selenium_mcg,
	a.iodine_mcg
FROM all_final_middle_table as b
LEFT JOIN final_combo_emiss as e
  ON b.food_name = e.food_name
LEFT JOIN averages_table as a
  ON b.food_name = a.food_name
),

--- calculate recommended allowance % for each male and female
kcal_recs as (
    SELECT DISTINCT
    f.food_name,
	CASE WHEN f.kcal IS NULL OR f.kcal = 0 THEN 0 ELSE AVG(f.kcal) OVER (PARTITION BY f.food_name) / 2500.0 END AS male_kcal_daily_rec,
    CASE WHEN f.kcal is null or f.kcal =0 THEN 0 ELSE AVG(f.kcal) OVER (PARTITION BY f.food_name) / 2000.0 END AS female_kcal_daily_rec
FROM final_table as f
),

water_recs  as (
    SELECT DISTINCT
    f.food_name,
	CASE WHEN f.water_g IS NULL OR f.water_g = 0 THEN 0 ELSE AVG(f.water_g) OVER (PARTITION BY f.food_name) / r.sex_factor_male END AS male_water_daily_rec,
    CASE WHEN f.water_g is null or f.water_g =0 THEN 0 ELSE AVG(f.water_g) OVER (PARTITION BY f.food_name) / r.sex_factor_female END AS female_water_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'water_g'
),

 fiber_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.fiber_g is null OR f.fiber_g =0 THEN 0 ELSE AVG(f.fiber_g) OVER (PARTITION BY f.food_name) / r.sex_factor_male END AS male_fiber_daily_rec,
  CASE WHEN f.fiber_g is null OR f.fiber_g =0 THEN 0 ELSE AVG(f.fiber_g) OVER (PARTITION BY f.food_name) / r.sex_factor_female END AS female_fiber_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'fiber_g'
),

 sodium_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.sodium_g is null OR f.sodium_g =0 THEN 0 ELSE AVG(f.sodium_g) OVER (PARTITION BY f.food_name)*10 / r.sex_factor_male END AS male_sodium_daily_rec,
  CASE WHEN f.sodium_g is null OR f.sodium_g =0 THEN 0 ELSE AVG(f.sodium_g) OVER (PARTITION BY f.food_name)*10 / r.sex_factor_female END AS female_sodium_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'sodium_mg'
),

 total_fat_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.total_fat_g is null OR f.total_fat_g =0 THEN 0 ELSE AVG(f.total_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_total_fat_daily_rec,
  CASE WHEN f.total_fat_g is null OR f.total_fat_g =0 THEN 0 ELSE AVG(f.total_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_total_fat_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'total_fat_g'
),

 sat_fat_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.saturated_fat_g is null OR f.saturated_fat_g =0 THEN 0 ELSE AVG(f.saturated_fat_g) OVER(PARTITION BY f.food_name)  /  r.sex_factor_male END AS male_saturated_fat_daily_rec,
  CASE WHEN f.saturated_fat_g is null OR f.saturated_fat_g =0 THEN 0 ELSE AVG(f.saturated_fat_g) OVER(PARTITION BY f.food_name)  /  r.sex_factor_female END AS female_saturated_fat_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'saturated_fat_g'
),


 mono_fat_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.monounsaturated_fat_g is null OR f.monounsaturated_fat_g =0 THEN 0 ELSE AVG(f.monounsaturated_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_monounsaturated_fat_daily_rec,
  CASE WHEN f.monounsaturated_fat_g is null OR f.monounsaturated_fat_g =0 THEN 0 ELSE AVG(f.monounsaturated_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_monounsaturated_fat_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'monounsaturated_fatty_acids_g'
),

 poly_fat_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.polyunsaturated_fat_g is null OR f.polyunsaturated_fat_g =0 THEN 0 ELSE AVG(f.polyunsaturated_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_polyunsaturated_fat_daily_rec,
  CASE WHEN f.polyunsaturated_fat_g is null OR f.polyunsaturated_fat_g =0 THEN 0 ELSE AVG(f.polyunsaturated_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_polyunsaturated_fat_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'polyunsaturated_fatty_acids_g'
),

 trans_fat_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.trans_fat_g is null OR f.trans_fat_g =0 THEN 0 ELSE AVG(f.trans_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_trans_fat_daily_rec,
  CASE WHEN f.trans_fat_g is null OR f.trans_fat_g =0 THEN 0 ELSE AVG(f.trans_fat_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_trans_fat_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'trans_fat_g'
),

 choles_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.cholesterol_mg is null OR f.cholesterol_mg =0 THEN 0 ELSE AVG(f.cholesterol_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_cholesterol_daily_rec,
  CASE WHEN f.cholesterol_mg is null OR f.cholesterol_mg =0 THEN 0 ELSE AVG(f.cholesterol_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS female_cholesterol_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'cholesterol_mg'
),

 protein_recs as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.protein_g is null OR f.protein_g =0 THEN 0 ELSE AVG(f.protein_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_protein_daily_rec,
  CASE WHEN f.protein_g is null OR f.protein_g =0 THEN 0 ELSE AVG(f.protein_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_protein_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'protein_g'
),

 carbs_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.carbohydrate_g is null OR f.carbohydrate_g =0 THEN 0 ELSE AVG(f.carbohydrate_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_carbs_daily_rec,
  CASE WHEN f.carbohydrate_g is null OR f.carbohydrate_g =0 THEN 0 ELSE AVG(f.carbohydrate_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_carbs_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'carbohydrate_g'
),

 total_sugars_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.total_sugars_g is null OR f.total_sugars_g =0 THEN 0 ELSE AVG(f.total_sugars_g) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_total_sugars_daily_rec,
  CASE WHEN f.total_sugars_g is null OR f.total_sugars_g =0 THEN 0 ELSE AVG(f.total_sugars_g) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_total_sugars_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'total_sugars_g'
),

 vit_a_iu_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_a_iu is null OR f.vitamin_a_iu =0 THEN 0 ELSE AVG(f.vitamin_a_iu) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_a_iu_daily_rec,
  CASE WHEN f.vitamin_a_iu is null OR f.vitamin_a_iu =0 THEN 0 ELSE AVG(f.vitamin_a_iu) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_a_iu_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_a_iu'
),

 vit_a_mcg_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_a_mcg is null OR f.vitamin_a_mcg =0 THEN 0 ELSE AVG(f.vitamin_a_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male  END AS male_vit_a_mcg_daily_rec,
  CASE WHEN f.vitamin_a_mcg is null OR f.vitamin_a_mcg =0 THEN 0 ELSE AVG(f.vitamin_a_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_female  END AS female_vit_a_mcg_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_a_mcg'
),

 vit_b1_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.thiamin_mg is null OR f.thiamin_mg =0 THEN 0 ELSE AVG(f.thiamin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b1_daily_rec,
  CASE WHEN f.thiamin_mg is null OR f.thiamin_mg =0 THEN 0 ELSE AVG(f.thiamin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b1_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'thiamin_mg'
),

 vit_b2_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.riboflavin_mg is null OR f.riboflavin_mg =0 THEN 0 ELSE AVG(f.riboflavin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b2_daily_rec,
  CASE WHEN f.riboflavin_mg is null OR f.riboflavin_mg =0 THEN 0 ELSE AVG(f.riboflavin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b2_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'riboflavin_mg'
),

 vit_b3_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.niacin_mg is null OR f.niacin_mg =0 THEN 0 ELSE AVG(f.niacin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b3_daily_rec,
  CASE WHEN f.niacin_mg is null OR f.niacin_mg =0 THEN 0 ELSE AVG(f.niacin_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b3_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'niacin_mg'
),

 vit_b4_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.choline_mg is null OR f.choline_mg =0 THEN 0 ELSE AVG(f.choline_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b4_daily_rec,
  CASE WHEN f.choline_mg is null OR f.choline_mg =0 THEN 0 ELSE AVG(f.choline_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b4_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'choline_mg'
),

 vit_b5_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.pantothenate_mg is null OR f.pantothenate_mg =0 THEN 0 ELSE AVG(f.pantothenate_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b5_daily_rec,
  CASE WHEN f.pantothenate_mg is null OR f.pantothenate_mg =0 THEN 0 ELSE AVG(f.pantothenate_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b5_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'pantothenate_mg'
),

 vit_b6_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_b6_mg is null OR f.vitamin_b6_mg =0 THEN 0 ELSE AVG(f.vitamin_b6_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b6_daily_rec,
  CASE WHEN f.vitamin_b6_mg is null OR f.vitamin_b6_mg =0 THEN 0 ELSE AVG(f.vitamin_b6_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b6_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_b6_mg'
),

 vit_b9_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.folate_mcg is null OR f.folate_mcg =0 THEN 0 ELSE AVG(f.folate_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b9_daily_rec,
  CASE WHEN f.folate_mcg is null OR f.folate_mcg =0 THEN 0 ELSE AVG(f.folate_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b9_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'folate_mcg'
),

 vit_b12_recs  as (
  SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_b12_mcg is null OR f.vitamin_b12_mcg =0 THEN 0 ELSE AVG(f.vitamin_b12_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_b12_daily_rec,
  CASE WHEN f.vitamin_b12_mcg is null OR f.vitamin_b12_mcg =0 THEN 0 ELSE AVG(f.vitamin_b12_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_b12_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_b12_mcg'
),

vit_c_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_c_mg is null OR f.vitamin_c_mg =0 THEN 0 ELSE AVG(f.vitamin_c_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_c_daily_rec,
  CASE WHEN f.vitamin_c_mg is null OR f.vitamin_c_mg =0 THEN 0 ELSE AVG(f.vitamin_c_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_c_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_c_mg'
),

 vit_d_iu_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_d_iu is null OR f.vitamin_d_iu =0 THEN 0 ELSE AVG(f.vitamin_d_iu) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_d_iu_daily_rec,
  CASE WHEN f.vitamin_d_iu is null OR f.vitamin_d_iu =0 THEN 0 ELSE AVG(f.vitamin_d_iu) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_d_iu_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_d_iu'
),

 vit_d_mcg_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_d_mcg is null OR f.vitamin_d_mcg =0 THEN 0 ELSE AVG(f.vitamin_d_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_d_mcg_daily_rec,
  CASE WHEN f.vitamin_d_mcg is null OR f.vitamin_d_mcg =0 THEN 0 ELSE AVG(f.vitamin_d_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_d_mcg_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_d_mcg'
),

 vit_e_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.vitamin_e_mg is null OR f.vitamin_e_mg =0 THEN 0 ELSE AVG(f.vitamin_e_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_vit_e_daily_rec,
  CASE WHEN f.vitamin_e_mg is null OR f.vitamin_e_mg =0 THEN 0 ELSE AVG(f.vitamin_e_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_vit_e_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'vitamin_e_mg'
),

 mi_lutein_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.lutein_mcg is null OR f.lutein_mcg =0 THEN 0 ELSE AVG(f.lutein_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_lutein_daily_rec,
  CASE WHEN f.lutein_mcg is null OR f.lutein_mcg =0 THEN 0 ELSE AVG(f.lutein_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS female_min_lutein_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'lutein_mcg'
),

 mi_potassium_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.potassium_mg is null OR f.potassium_mg =0 THEN 0 ELSE AVG(f.potassium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_potassium_daily_rec,
  CASE WHEN f.potassium_mg is null OR f.potassium_mg =0 THEN 0 ELSE AVG(f.potassium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_potassium_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'potassium_mg'
),

 mi_calcium_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.calcium_mg is null OR f.calcium_mg =0 THEN 0 ELSE AVG(f.calcium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_calcium_daily_rec,
  CASE WHEN f.calcium_mg is null OR f.calcium_mg =0 THEN 0 ELSE AVG(f.calcium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_calcium_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'calcium_mg'
),

 mi_magnesium_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.magnesium_mg is null OR f.magnesium_mg =0 THEN 0 ELSE AVG(f.magnesium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_magnesium_daily_rec,
  CASE WHEN f.magnesium_mg is null OR f.magnesium_mg =0 THEN 0 ELSE AVG(f.magnesium_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_magnesium_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'magnesium_mg'
),

 mi_phosphorous_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.phosphorous_mg is null OR f.phosphorous_mg =0 THEN 0 ELSE AVG(f.phosphorous_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_phosphorous_daily_rec,
  CASE WHEN f.phosphorous_mg is null OR f.phosphorous_mg =0 THEN 0 ELSE AVG(f.phosphorous_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_phosphorous_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'phosphorous_mg'
),

 mi_iron_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.iron_mg is null OR f.iron_mg =0 THEN 0 ELSE AVG(f.iron_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_iron_daily_rec,
  CASE WHEN f.iron_mg is null OR f.iron_mg =0 THEN 0 ELSE AVG(f.iron_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_iron_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'iron_mg'
),

 mi_copper_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.copper_mg is null OR f.copper_mg =0 THEN 0 ELSE AVG(f.copper_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_copper_daily_rec,
  CASE WHEN f.copper_mg is null OR f.copper_mg =0 THEN 0 ELSE AVG(f.copper_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_copper_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'copper_mg'
),

 mi_zinc_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.zinc_mg is null OR f.zinc_mg =0 THEN 0 ELSE AVG(f.zinc_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_zinc_daily_rec,
  CASE WHEN f.zinc_mg is null OR f.zinc_mg =0 THEN 0 ELSE AVG(f.zinc_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_zinc_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'zinc_mg'
),

 mi_chloride_recs  as (
    SELECT DISTINCT
  f.food_name,
  CASE WHEN f.chloride_mg is null OR f.chloride_mg =0 THEN 0 ELSE AVG(f.chloride_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_chloride_daily_rec,
  CASE WHEN f.chloride_mg is null OR f.chloride_mg =0 THEN 0 ELSE AVG(f.chloride_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_chloride_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'chloride_mg'
),

 mi_manganese_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.manganese_mg is null OR f.manganese_mg =0 THEN 0 ELSE AVG(f.manganese_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_manganese_daily_rec,
  CASE WHEN f.manganese_mg is null OR f.manganese_mg =0 THEN 0 ELSE AVG(f.manganese_mg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_manganese_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'manganese_mg'
),

 mi_selenium_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.selenium_mcg is null OR f.selenium_mcg =0 THEN 0 ELSE AVG(f.selenium_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_selenium_daily_rec,
  CASE WHEN f.selenium_mcg is null OR f.selenium_mcg =0 THEN 0 ELSE AVG(f.selenium_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_female END AS female_min_selenium_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'selenium_mcg'
),

 mi_iodine_recs  as (
   SELECT DISTINCT
  f.food_name,
  CASE WHEN f.iodine_mcg is null OR f.iodine_mcg =0 THEN 0 ELSE AVG(f.iodine_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS male_min_iodine_daily_rec,
  CASE WHEN f.iodine_mcg is null OR f.iodine_mcg =0 THEN 0 ELSE AVG(f.iodine_mcg) OVER(PARTITION BY f.food_name) / r.sex_factor_male END AS female_min_iodine_daily_rec
FROM final_table as f
LEFT JOIN public.recommended_daily_intakes as r
  ON r.nutrient_vitamin = 'iodine_mcg'
),

-- combine percentages
all_recs as (
SELECT
	water_recs.food_name,
	male_kcal_daily_rec,
	female_kcal_daily_rec,
	male_water_daily_rec,
	female_water_daily_rec,
	male_fiber_daily_rec,
	female_fiber_daily_rec,
	male_sodium_daily_rec,
	female_sodium_daily_rec,
	male_total_fat_daily_rec,
	female_total_fat_daily_rec,
	male_saturated_fat_daily_rec,
	female_saturated_fat_daily_rec,
	male_monounsaturated_fat_daily_rec,
	female_monounsaturated_fat_daily_rec,
	male_polyunsaturated_fat_daily_rec,
	female_polyunsaturated_fat_daily_rec,
	male_trans_fat_daily_rec,
	female_trans_fat_daily_rec,
	male_cholesterol_daily_rec,
	female_cholesterol_daily_rec,
	male_protein_daily_rec,
	female_protein_daily_rec,
	male_carbs_daily_rec,
	female_carbs_daily_rec,
	male_total_sugars_daily_rec,
	female_total_sugars_daily_rec,
	male_vit_a_iu_daily_rec,
	female_vit_a_iu_daily_rec,
	male_vit_a_mcg_daily_rec,
	female_vit_a_mcg_daily_rec,
	male_vit_b1_daily_rec,
	female_vit_b1_daily_rec,
	male_vit_b2_daily_rec,
	female_vit_b2_daily_rec,
	male_vit_b3_daily_rec,
	female_vit_b3_daily_rec,
	male_vit_b4_daily_rec,
	female_vit_b4_daily_rec,
	male_vit_b5_daily_rec,
	female_vit_b5_daily_rec,
	male_vit_b6_daily_rec,
	female_vit_b6_daily_rec,
	male_vit_b9_daily_rec,
	female_vit_b9_daily_rec,
	male_vit_b12_daily_rec,
	female_vit_b12_daily_rec,
	male_vit_c_daily_rec,
	female_vit_c_daily_rec,
	male_vit_d_iu_daily_rec,
	female_vit_d_iu_daily_rec,
	male_vit_d_mcg_daily_rec,
	female_vit_d_mcg_daily_rec,
	male_vit_e_daily_rec,
	female_vit_e_daily_rec,
	male_min_lutein_daily_rec,
	female_min_lutein_daily_rec,
	male_min_potassium_daily_rec,
	female_min_potassium_daily_rec,
	male_min_calcium_daily_rec,
	female_min_calcium_daily_rec,
	male_min_magnesium_daily_rec,
	female_min_magnesium_daily_rec,
	male_min_phosphorous_daily_rec,
	female_min_phosphorous_daily_rec,
	male_min_iron_daily_rec,
	female_min_iron_daily_rec,
	male_min_copper_daily_rec,
	female_min_copper_daily_rec,
	male_min_zinc_daily_rec,
	female_min_zinc_daily_rec,
	male_min_chloride_daily_rec,
	female_min_chloride_daily_rec,
	male_min_manganese_daily_rec,
	female_min_manganese_daily_rec,
	male_min_selenium_daily_rec,
	female_min_selenium_daily_rec,
	male_min_iodine_daily_rec,
	female_min_iodine_daily_rec
FROM water_recs
	INNER JOIN kcal_recs ON water_recs.food_name = kcal_recs.food_name
	INNER JOIN fiber_recs ON water_recs.food_name =  fiber_recs.food_name
	INNER JOIN sodium_recs ON water_recs.food_name =  sodium_recs.food_name
	INNER JOIN total_fat_recs ON water_recs.food_name =  total_fat_recs.food_name
	INNER JOIN sat_fat_recs ON water_recs.food_name =  sat_fat_recs.food_name
	INNER JOIN mono_fat_recs ON water_recs.food_name =  mono_fat_recs.food_name
	INNER JOIN poly_fat_recs ON water_recs.food_name =  poly_fat_recs.food_name
	INNER JOIN trans_fat_recs ON water_recs.food_name =  trans_fat_recs.food_name
	INNER JOIN choles_recs ON water_recs.food_name =  choles_recs.food_name
	INNER JOIN protein_recs ON water_recs.food_name =  protein_recs.food_name
	INNER JOIN carbs_recs ON water_recs.food_name =  carbs_recs.food_name
	INNER JOIN total_sugars_recs ON water_recs.food_name =  total_sugars_recs.food_name
	INNER JOIN vit_a_iu_recs ON water_recs.food_name =  vit_a_iu_recs.food_name
	INNER JOIN vit_a_mcg_recs ON water_recs.food_name =  vit_a_mcg_recs.food_name
	INNER JOIN vit_b1_recs ON water_recs.food_name =  vit_b1_recs.food_name
	INNER JOIN vit_b2_recs ON water_recs.food_name =  vit_b2_recs.food_name
	INNER JOIN vit_b3_recs ON water_recs.food_name =  vit_b3_recs.food_name
	INNER JOIN vit_b4_recs ON water_recs.food_name =  vit_b4_recs.food_name
	INNER JOIN vit_b5_recs ON water_recs.food_name =  vit_b5_recs.food_name
	INNER JOIN vit_b6_recs ON water_recs.food_name =  vit_b6_recs.food_name
	INNER JOIN vit_b9_recs ON water_recs.food_name =  vit_b9_recs.food_name
	INNER JOIN vit_b12_recs ON water_recs.food_name =  vit_b12_recs.food_name
	INNER JOIN vit_c_recs ON water_recs.food_name =  vit_c_recs.food_name
	INNER JOIN vit_d_iu_recs ON water_recs.food_name =  vit_d_iu_recs.food_name
	INNER JOIN vit_d_mcg_recs ON water_recs.food_name =  vit_d_mcg_recs.food_name
	INNER JOIN vit_e_recs ON water_recs.food_name =  vit_e_recs.food_name
	INNER JOIN mi_lutein_recs ON water_recs.food_name =  mi_lutein_recs.food_name
	INNER JOIN mi_potassium_recs ON water_recs.food_name =  mi_potassium_recs.food_name
	INNER JOIN mi_calcium_recs ON water_recs.food_name =  mi_calcium_recs.food_name
	INNER JOIN mi_magnesium_recs ON water_recs.food_name =  mi_magnesium_recs.food_name
	INNER JOIN mi_phosphorous_recs ON water_recs.food_name =  mi_phosphorous_recs.food_name
	INNER JOIN mi_iron_recs ON water_recs.food_name =  mi_iron_recs.food_name
	INNER JOIN mi_copper_recs ON water_recs.food_name =  mi_copper_recs.food_name
	INNER JOIN mi_zinc_recs ON water_recs.food_name =  mi_zinc_recs.food_name
	INNER JOIN mi_chloride_recs ON water_recs.food_name =  mi_chloride_recs.food_name
	INNER JOIN mi_manganese_recs ON water_recs.food_name =  mi_manganese_recs.food_name
	INNER JOIN mi_selenium_recs ON water_recs.food_name =  mi_selenium_recs.food_name
	INNER JOIN mi_iodine_recs ON water_recs.food_name =  mi_iodine_recs.food_name
),

-- create final table combining extracted ingredient, cooking processes, emissions, and recommend nutritional allowance %
final_final as (
SELECT 
  DISTINCT
	f.raw_ingredient,
	f.food_name,
	f.dish_process,
	f.main_ingredient_process,
	f.dish_name,
	f.family_ingredient_or_final_process,
	f.group_code,
	f.group_name,
	--f.data_source,
	f.emissions_serving_size,
	f.emissions_calories_content,
	f.emissions_protein_content,
	f.emissions_fat_content,
	----- macros
	f.kcal,
	a.male_kcal_daily_rec,
	a.female_kcal_daily_rec,
	f.water_g,
	a.male_water_daily_rec,
	a.female_water_daily_rec,
	f.fiber_g,
	a.male_fiber_daily_rec,
	a.female_fiber_daily_rec,
	f.sodium_g,
	a.male_sodium_daily_rec,
	a.female_sodium_daily_rec,
	f.total_fat_g,
	a.male_total_fat_daily_rec,
	a.female_total_fat_daily_rec,
	f.saturated_fat_g,
	a.male_saturated_fat_daily_rec,
	a.female_saturated_fat_daily_rec,
	f.monounsaturated_fat_g,
	a.male_monounsaturated_fat_daily_rec,
	a.female_monounsaturated_fat_daily_rec,
	f.polyunsaturated_fat_g,
	a.male_polyunsaturated_fat_daily_rec,
	a.female_polyunsaturated_fat_daily_rec,
	f.trans_fat_g,
	a.male_trans_fat_daily_rec,
	a.female_trans_fat_daily_rec,
	f.cholesterol_mg,
	a.male_cholesterol_daily_rec,
	a.female_cholesterol_daily_rec,
	f.protein_g,
	a.male_protein_daily_rec,
	a.female_protein_daily_rec,
	f.carbohydrate_g,
	a.male_carbs_daily_rec,
	a.female_carbs_daily_rec,
	f.total_sugars_g,
	a.male_total_sugars_daily_rec,
	a.female_total_sugars_daily_rec,
	f.glucose_g,
	f.galactose_g,
	f.fructose_g,
	f.sucrose_g,
	f.maltose_g,
	f.lactose_g,
	--- vitamins
	---a
	f.retinol_mcg,
	f.carotene_alpha_mcg,
	f.carotene_beta_mcg,
	f.vitamin_a_iu,
	a.male_vit_a_iu_daily_rec,
	a.female_vit_a_iu_daily_rec,
	f.vitamin_a_mcg,
	a.male_vit_a_mcg_daily_rec,
	a.female_vit_a_mcg_daily_rec,
	---b
	f.thiamin_mg as vit_b1_mg,
	a.male_vit_b1_daily_rec,
	a.female_vit_b1_daily_rec,
	f.riboflavin_mg as vit_b2_mg,
	a.male_vit_b2_daily_rec,
	a.female_vit_b2_daily_rec,
	f.niacin_mg as vit_b3_mg,
	a.male_vit_b3_daily_rec,
	a.female_vit_b3_daily_rec,
	f.choline_mg as vit_b4_mg,
	a.male_vit_b4_daily_rec,
	a.female_vit_b4_daily_rec,
	f.pantothenate_mg as vit_b5_mg,
	a.male_vit_b5_daily_rec,
	a.female_vit_b5_daily_rec,
	f.pantothenic_acid_mg,
	f.vitamin_b6_mg,
	a.male_vit_b6_daily_rec,
	a.female_vit_b6_daily_rec,
	f.biotin_mcg as vit_b7_mcg,
	f.folate_mcg as vit_b9_mcg,
	a.male_vit_b9_daily_rec,
	a.female_vit_b9_daily_rec,
	f.vitamin_b12_mcg,
	a.male_vit_b12_daily_rec,
	a.female_vit_b12_daily_rec,
	---c
	f.vitamin_c_mg,
	a.male_vit_c_daily_rec,
	a.female_vit_c_daily_rec,
	---d
	f.vitamin_d_iu,
	a.male_vit_d_iu_daily_rec,
	a.female_vit_d_iu_daily_rec,
	f.vitamin_d_mcg,
	a.male_vit_d_mcg_daily_rec,
	a.female_vit_d_mcg_daily_rec,
	---e
	f.vitamin_e_mg,
	a.male_vit_e_daily_rec,
	a.female_vit_e_daily_rec,
	---k
	f.vitamin_k_mcg,
	--- minerals
	f.tryptophan_g,
	f.lutein_mcg,
	a.male_min_lutein_daily_rec,
	a.female_min_lutein_daily_rec,
	f.potassium_mg,
	a.male_min_potassium_daily_rec,
	a.female_min_potassium_daily_rec,
	f.calcium_mg,
	a.male_min_calcium_daily_rec,
	a.female_min_calcium_daily_rec,
	f.magnesium_mg,
	a.male_min_magnesium_daily_rec,
	a.female_min_magnesium_daily_rec,
	f.phosphorous_mg,
	a.male_min_phosphorous_daily_rec,
	a.female_min_phosphorous_daily_rec,
	f.iron_mg,
	a.male_min_iron_daily_rec,
	a.female_min_iron_daily_rec,
	f.copper_mg,
	a.male_min_copper_daily_rec,
	a.female_min_copper_daily_rec,
	f.zinc_mg as zinc_mg,
	a.male_min_zinc_daily_rec,
	a.female_min_zinc_daily_rec,
	f.chloride_mg,
	a.male_min_chloride_daily_rec,
	a.female_min_chloride_daily_rec,
	f.manganese_mg,
	a.male_min_manganese_daily_rec,
	a.female_min_manganese_daily_rec,
	f.selenium_mcg,
	a.male_min_selenium_daily_rec,
	a.female_min_selenium_daily_rec,
	f.iodine_mcg,
	a.male_min_iodine_daily_rec,
	a.female_min_iodine_daily_rec
FROM final_table as f
  LEFT JOIN all_recs as a ON f.food_name = a.food_name
)

SELECT * 
INTO nutrition_detail_final
FROM final_final;

--- added more to the list of processes to factor in others that remove or add ingredients to the raw incredients

  processing_processes AS (SELECT ARRAY['added','air-popped','baked','batter-dipped','battered','blanched','blend','boiled','braised','breaded','brined','broiled','canned','chilled','chopped','coated','compressed','concentrate','condensed','contains','cooked','covered','crisped','cured','cut','dehydrated','dipped','distilled','drained','dried','dried-frozen','enriched','extract','filled','flavored','freeze-dried','fresh','fried','frozen','granulated','grilled','ground','heated','homemade','hydrogenated','imitation','in batter','in crumbs','in flour','microwaved','minced','oil-popped','oven-heated','pan-broiled','pan-fried','pasteurized','pre-cooked','preserved','puffed','raw','re-fried','read-to-eat','reconstituted','roasted','salted','seasoned','seeded','steamed','stewed','stewing','stir-fried','stuffed','sweetened','toasted','unsalted','whole','bottled', 'unprepared', 'unenriched','uncooked' 
    ] as process),
  raw_processes AS (SELECT ARRAY['chilled','dehydrated','drained','dried','freeze-dried','frozen','fresh','ground','raw','uncooked','whole','chopped','concentrate','condensed','cured','cut','granulated','preserved'
    ] as   process)

SELECT *
FROM final_final ff
WHERE EXISTS (
  SELECT 1
  FROM raw_processes, UNNEST(process) AS rp
  WHERE ff.dish_process ILIKE '%' || rp || '%'
)
AND emissions_serving_size is null and group_name is not null
ORDER BY group_name;

DELETE FROM public.nutrition_detail_final

DROP TABLE public.nutrition_detail_final