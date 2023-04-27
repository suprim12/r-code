###############################################################################

#Install the mongolite library
install.packages("mongolite")

install.packages("jsonlite")
install.("tidyverse")

#Import the mongolit library
library(mongolite)

library(jsonlite)

library(tidyverse)

#library(stringr)

#perpare to connect to mongoDB
connection_string = 
  'mongodb+srv://kevinlearnsit:comp8031@comp8031.0izbk9g.mongodb.net/
?retryWrites=true&w=majority'

#collection : "listingsAndReviews"
#connect and get the collection
bnb_collection = mongo(collection="listingsAndReviews", db="sample_airbnb", 
                       url=connection_string)

#show how many items we have
bnb_collection$count()

#have a look at the first item
bnb_collection$iterate()$one() 

###############################################################################
#Just take a look like how our data looks like, and also we can see if the data
#is reasonable. 
###############################################################################
test <- bnb_collection$find(
  query = '{"property_type" : "House", "address.country" : "Australia"}', 
  fields = '{"name" : true, "price": true,
  "review_scores.review_scores_rating": true}',
  limit = 20
)

#See what we got
print(test)

###############################################################################
##Get the information we plan to use
###############################################################################
sample_bnb <- bnb_collection$find(
  query = '{
  "address.country" : {
      "$in" : ["United States"]
    }
  }', 
  fields = '{
  "name" :                    true,
  "price" :                   true,
  "cleaning_fee" :            true,
  "security_deposit" :        true,
  "extra_people" :            true,
  "guests_included" :         true,
  "property_type" :           true,
  "bedrooms" :                true,
  "beds" :                    true,
  "address.country" :         true,
  "calendar_last_scraped" :   true,
  "first_review" :            true,
  "last_review" :             true,
  "review_scores.review_scores_rating": true
  }',
)

# Make the data tidy and limit the length of strings to avoid unnecessary delay
# for the further process
tb <- tibble(sample_bnb)

# tb[["name"]] <- str_sub(tb[["name"]], end = 32)
# tb[["name"]] <- str_pad(tb[["name"]], width = 32, side = "right")



### START Tidying up the data (GOLA0011) ---------------------


##  Rename for fields eg: rating
tb <- tb %>% 
  rename(name = "name", price = "price", property_type = "property_type", 
         bedrooms = "bedrooms", beds = "beds", rating = "review_scores.review_scores_rating")

## convert numeric related fileds to numerice type
tb$price <- as.numeric(gsub("[^0-9.]", "", tb$price))
tb$minimum_nights <- as.numeric(gsub("[^0-9.]", "", tb$minimum_nights))
tb$maximum_nights <- as.numeric(gsub("[^0-9.]", "", tb$maximum_nights))
tb$security_deposit <- as.numeric(gsub("[^0-9.]", "", tb$security_deposit))
tb$cleaning_fee <- as.numeric(gsub("[^0-9.]", "", tb$cleaning_fee))
tb$extra_people <- as.numeric(gsub("[^0-9.]", "", tb$extra_people))


## convert date related to date type
tb$first_review <- as.Date(tb$first_review)
tb$last_review <- as.Date(tb$last_review)

## convert repetated data into one
tb <- tb %>%
  mutate(last_scraped = as.Date(as.numeric(last_scraped["$date"]["$numberLong"]) / 1000, origin = "1970-01-01")) %>%
  mutate(calendar_last_scraped = as.Date(as.numeric(calendar_last_scraped["$date"]["$numberLong"]) / 1000, origin = "1970-01-01")) %>%
  unite("scraped_date", last_scraped, calendar_last_scraped, sep = ", ")

## new field price_per_bed
tb <- tb %>% 
  mutate(price_per_bed = price / beds)

## Filter "price_per_bed" field is greater than 500
tb <- tb %>% 
  filter(price_per_bed <= 500)


### Remove any rows with missing values
tb <- tb %>% 
  drop_na()

### END Tidying up the data  -------------------------------

# Now prepare to export our data to a local file
# Flatten the data
flat_data <- jsonlite::flatten(tb)

# Specify the encoding for the CSV file (e.g., UTF-8)
encoding <- "UTF-16"

# Write the flat_data data frame to a CSV file with the specified encoding
write.csv(flat_data, file = "sample_bnb_01.csv", row.names = FALSE, 
          fileEncoding = encoding)




###############################################################################

###############################################################################












