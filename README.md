# Welcome Propert Tax Assessment Database Super Users!

This repo was created to help answer common questions about becoming a super user who can query the Utah property tax assessment database. This repo also contains helpful sql scripts and other templates for common analytics. 

## What is a Super User? 

A Super User has the ability to access the raw data stored in our database using SQL queries. If you do not have the technical skills or expertise to be a super user, you can hire a person with those skills and they can be given super user access to perform analysis for you. 

Super Users are billed for the queries that they perform in their Google Cloud Project. Google Cloud Platform has a “Free Tier” of services which covers up to 1TB of queries per month (Depending on the size and complexity of the query, this usually covers 10-20  complex queries), however any queries beyond that limit will be charged to the super user. Billing is complexly calculated by Google, but a good rule of thumb is ~$5 per 1TB of data queried. If you notice your bill increasing or costing lots of money please reach out and we can show you how to partition your tables and query in a way that reduces costs.
## What is the process for becoming a Super User? 

To become a super user, there are two steps:

1. Please send  Alex Nielson a valid gmail account you want to gain access to the data. If you do not have a gmail account, you will need to set one up for your user or organization. 

2. Next, set up a Google Cloud platform billing account and project for the valid gmail account you sent Alexander Nielson. The steps to do this are detailed in this pdf: https://docs.google.com/document/d/1H8U5pGS4iY7J6YyuPrGwSBbECM2nm8Q3MdNNmR9aqgc/edit?usp=sharing


## How do I perform queries in R, Python or a different software?

If you wish to perform your analysis in R, Python, or other programming language, 
then you must set up a special credential file called a "Service Account". 

This tutorial will document how you can set up a service account: https://docs.google.com/document/d/1PwEITu7y0xuq9flRPcKJ3vlBTUSI_Epq43iszHNgGTA/edit?usp=sharing Please make sure you to send the service account's email once it is created since it will need to be whitelisted in our database to get access to the data. 

## I still have questions

If you have other questions please email Alexander Nielson (alexnielson@utah.gov).

If you wish to request a one time 30 minute database demo and explanation of the database, this can be scheduled with Alexander Nielson (alexnielson@utah.gov) per his schedule. These meetings are at the Office of the State Auditor’s whim and convenience. If we do not have the time or resources, you may not get a tutorial meeting. 

## Table Data Dictionary

There is a "public" and "protected" version of the database. While all the data sent to the Office of the State Auditor is public data, and not according to the state code protected or private information, there is a change in ability and scope to search owner information using this database. 

After receiving feedback from various stakeholders, the OSA has decided to only allow county assessors and other required governmental employees access to the owner information. Super users do not by default get access to owner information and a few additional fields related to the property that are non-essential. 

To see the fields available to you as a super user, please visit the schema here: 
https://docs.google.com/spreadsheets/d/1CNwGEL5QfvNGaslamYfF-QR28baPZdqh5yfG3_cLgNE/edit?usp=sharing


If you are from a state agency or related researcher, you may be able to gain access to owner fields if it is approved by the State Auditor. To see a schema with ALL possible fields available visit this schema here: 

https://docs.google.com/spreadsheets/d/1QXT760-BFgNnbljIxuVaQ1nrOYVDs03wuMF0u51RCik/edit?usp=sharing

Please contact Alex Nielson (alexnielson@utah.gov) if you need access to these fields and we can discuss further how to help you achieve your research/statutory obligations.  

## Known Data Quality issues and explanations
coming soon.