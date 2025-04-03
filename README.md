# Purpose
The purpose of this project is to:
- Educate myself on best foods to eat for my health as I transition to a plant-based diet
- Educate myself on the the emissions of all food production
- Help myself and others make more informed decisions about food

# Approach

Since a primary the purpose of this project is to educate myself on how I can eat/live better, I have approached it as someone who lives in UK, with, theoretically, access to a large variety of food.

1. I knew the dataset needed to be as large as possible, to maximize my deliciousness horizon.

Since speaking to friends and family about my decision to become more plant-based, I have had alot of statements / questions along the lines of:
- "But what about protein?"
- "How will you get enough B12, Iron or Calcium?"
- "You'll feel tired" / "It's boring" / "It's more work to cook something nice"
- "The whole world can't be vegan"

Whilst some of these may have been legitimate concerns / points I wondered if any were backed by actual data. What became aparent was that I lived in a society that largely favours convenience for time-saving sake over enjoying the process, when it came to feeding ourselves. Together with huge lobbying groups advocating for all animal-based products, one could also expect a degree of misinformation and or exageration is present in these views I encountered.

2. I realised my end-result needed to be easily understandeable and could be rapidly used to make decisions about nutrition. The dataset would not only have to cover as many of the nutrients, vitamins and minerals, whilst making it understeable to the user / reader of the data how to decide for themselves which foods to eat. 
    - What are the most important nutrients, vitamins and minerals, and why are they so important? What do various 'food plans/diets' focus on?
    - What happens to these when the food is cooked versus when food is raw?
    - How do you calculate the 'value' of a food in an understandeable and relatable?
    - What do the current food tracking apps achieve and what are their pitfalls?

CO2 Emissions have been a major discussion point that has rightly been covered at length in western media. Many reports, including the [The Living Planet Report 2024 by WWF](https://livingplanet.panda.org/en-GB/) points how agriculture accounts for most (~60%) of all global CO2 Emissions worldwide.

3. The best way to solve a problem is to focus on the biggest contributing factor of that problem.
4. How do I contextualise the kilograms of CO2 emissions produced by food chains? How can people understand the magnitude of the emissions?
5. The project needs to be based on reliable data that is publicly available.


# Goals
1. Build a facts table that provides me a breakdown of nutrients, vitamins and minerals for as many foods as I can
2. Attribute Co2 emissions data to each of the foods, covering each stage of the farm to shop process (excluding emissions used for taking food from shop to plate)
3. Build a dataset for ranking foods based on a variety of nutrion and health goals
4. Enhance the ranking by incorporating their impact on the environement
5. Build a tool to help people make more informed decisions about food

# Laying the foundations 

With the purpose, apporaches and goals in mind, I set about gathering the data. one of the conerns I had was the reliability of the data sources, so I had to ensure I gathered several to ensure a satsifactory degree of confidence that the nutritional facts of the foods were accurate to what most people would experience.

For nutrients data by food:
- [McCance and Widdowson’s The Composition of Foods Integrated Dataset 2021](https://www.gov.uk/government/publications/composition-of-foods-integrated-dataset-cofid) - published by Public Health England (gov website)
- [Nutritional values for common foods and products](https://www.kaggle.com/datasets/trolukovich/nutritional-values-for-common-foods-and-products) - Kaggle dataset based on web scraped of: https://www.nutritionvalue.org/

For emissions:
- [Environmental Impact of Food Production](url) - Our World in Data 2022

# Data cleaning process

Since the underlying data is a combination of seperate sources, with possible duplications or errors, we need to account for these and extract further dimensions for each index, therefore the data cleaning process will be as follows, where each step is a seperate Common Table Expression (CTE)

## Food categorisation and nutrient data
1.  extract words that define the food process (eg. 'raw', 'cooked with salt' ) included in the `food_name`, this will be used to categorise data and help normalise nutrient values proportionally
2.  ensure nutrient values are consitent data type format and convert values as required for consistency (eg. from milligram to grams) and create `process` column using previous step
3.  extract the `raw_ingredient` for each row, where a single ingredient is returned; `main_ingredient_process` the main food processing method for each raw_ingredient or a descriptive dimension of raw_ingredients; `family_ingredient_or_process` if the food_name includes a non-raw process then this column will give details of this process
4.  clean data from previous step, removing whitespace and add `dish_name`, this is detail for raw_ingredient so (eg. if food_name is 'banana bread' then raw_ingredient = 'banana' and dish_name = 'bread')
5.  for specific group_name that have non-descriptive raw_ingredient (eg. 'raw nuts' instead of 'raw peanuts', 'raw brazil nuts' etc) we need to improve the raw_ingredient extraction so that we maximize the variety of different raw_ingredient that can be categorised; we also clean `group_name` whcih was manually added to the USA dataset to match the categories of the UK dataset; `family_names` is created manually by assigning from raw_ingredients, these are not one-to-one relationships, instead a raw-ingredient can come under several family_names
6.  for the uk dataset, some nutritional have placeholders, where there is significant variation in the values found the value = 99, where the are only trace amounts of the nutrients the value = 0.0001. We will use raw_ingredient to join on the values from the US source to replace these placeholders where possible, by taking the median value for each nutrient and assigning it to the UK rows (N.B this is not full proof but will limit the amount of placeholders and help with consistency in data when aggregated at group_name level downstream)
7.  combine all previous steps and add replace nulls with 0 for all nutrients

## Co2 Emissions data
8. whilst we will have overall emissions data for each group_name, we might not have the specific breakdown of emissions based on the step in the supply chain to produce the food, so we take the median value for each at a group_name level. We eventually will have the following columns, which we can use downstream for deeper analysis`food_emissions_land_use_kgCo2_perkg` , `food_emissions_farm_kgCo2_perkg`, `food_emissions_animal_feed_kgCo2_perkg`, `food_emissions_processing_kgCo2_perkg`, `food_emissions_transport_kgCo2_perkg`, `food_emissions_retail_kgCo2_perkg`, `food_emissions_packaging_kgCo2_perk`, `food_emissions_losses_kgCo2_perkg`, `total_emissions`
9. we join raw_foods_global_emmissions_kgCO2_perkg table to the raw_ingredients to normalise the total emissions based on various variables  to enable like-for-like comparisons downstream. Each will have  `emissions_pserving`, `emissions_pkg`, `emissions_p1kcalories`, `emissions_p100gprotein`, `emissions_p100gfat`; we also add the values for protein, calories and fat to create these emissions columns if they don't already have them with the join 
10. for rows that have no emissions data when joined using raw_ingredient, then we estimate these by joining using group_name and re-created `emissions_pserving`, `emissions_pkg`, `emissions_p1kcalories`, `emissions_p100gprotein`, `emissions_p100gfat` using `total_emissions` the protein, calories and fat values from previous step --- this will affect approximately 10% of rows
11. combine all previous CTEs to a final table of emissions **using food_name**
12. if still no emissions data the combine previous CTEs to a final table **using group_name**
13. coalesce values in step 11 and 12, **but prioritise the values from step 11**

## Daily recommendation data and final table creation
14. combine CTEs from step 13 and step 7
15. create a CTE 39 of the 60 nutrients and calculate the % of daily recommended coverage for males and females
16. combine all the CTEs from previous step together 
17. combine CTEs from steps 16 and 14 to form the final table with food categorisation, emissions and recommended daily allowance data

