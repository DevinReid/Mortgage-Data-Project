-- SQLite
SELECT AmortTerm, COUNT(*) AS LoanCount
FROM mortgages
GROUP BY AmortTerm
ORDER BY AmortTerm;



