--USE PersonDatabase;

/*********************
Hello! 
Please use the test data provided in the file 'PersonDatabase' to answer the following
questions. Please also import the dbo.Contacts flat file to a table for use. 
All answers should be executable on a MS SQL Server 2012 instance. 
***********************
QUESTION 1
The table dbo.Risk contains calculated risk scores for the population in dbo.Person. Write a 
query or group of queries that return the patient name, and their most recent risk level(s). 
Any patients that dont have a risk level should also be included in the results. 
**********************/
WITH MostRecentRisk as (
			
		SELECT
			PersonID,
			MAX(RiskDateTime) as MostRecentDate
		FROM
			DBO.Risk
		GROUP BY PersonID
	) 

SELECT 
	a.PersonID,
	a.PersonName,
	c.RiskLevel
FROM dbo.Person a
LEFT JOIN MostRecentRisk b on a.PersonID = b.PersonID
LEFT JOIN dbo.Risk c on b.PersonID = c.PersonID 
	AND b.MostRecentDate = c.RiskDateTime;




/**********************
QUESTION 2
The table dbo.Person contains basic demographic information. The source system users 
input nicknames as strings inside parenthesis. Write a query or group of queries to 
return the full name and nickname of each person. The nickname should contain only letters 
or be blank if no nickname exists.
**********************/

SELECT 
/**case statement checks to see if there is a value to the right of the open parenthesis. 
if so, it removes than parenthesized value and any traces of it**/
CASE
	WHEN LEN(RIGHT(PersonName, charindex('(', PersonName))) > 0
	THEN TRIM('( )' FROM REPLACE(PersonName, '(' + SUBSTRING(PersonName,CHARINDEX('(', PersonName)+1,CHARINDEX(')', PersonName)-CHARINDEX('(', PersonName)-1) + ')', ''))
	ELSE PersonName
END as PersonName,
CASE
	WHEN CHARINDEX('(', PersonName) > 0 AND LEN(SUBSTRING(PersonName,CHARINDEX('(', PersonName)+1,CHARINDEX(')', PersonName)-CHARINDEX('(', PersonName)-1)) > 0
	THEN SUBSTRING(PersonName,CHARINDEX('(', PersonName)+1,CHARINDEX(')', PersonName)-CHARINDEX('(', PersonName)-1)
END as NickName
FROM Person;


/**********************
QUESTION 6
Write a query to return risk data for all patients, all payers 
and a moving average of risk for that patient and payer in dbo.Risk. 
**********************/

SELECT DISTINCT 
	a.PersonID,
	PersonName,
	AttributedPayer,
	RiskDateTime,
	RiskScore,
	AVG(RiskScore) OVER (PARTITION BY a.PersonID, AttributedPayer ORDER BY RiskDateTime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as UpdatedRiskScoreMovingAverage
FROM
	dbo.Risk a
LEFT JOIN Person b on a.PersonID = b.PersonID
ORDER BY PersonID, RiskDateTime ASC;
