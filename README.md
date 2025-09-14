this is supposed to be an app where you can swipe left and right to join different columns in two datasets(csvs)

it would profile the columns and score the compatibility of different columns.
You could then swipe left and right (in a webapp) to choose which columns to join.

I have only done the profiling/matching CLI in GO.

Not finished, the profiling/matching doesnt work well mainly because: 
1. The profiling doesnt handle dates that are not of standard format well
2. The matching doesnt provide useful compatibility scores between different columns
3. In real life, there is no good usecase. Most datasets would have little to no overlap between columns. 

