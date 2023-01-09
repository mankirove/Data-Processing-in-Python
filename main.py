import os
import numpy as np
import pandas as pd
import sqlite3
import tempfile

Badges = pd.read_csv(r'C:\Users\user\PycharmProjects\dataprocessing2\Badges.csv')
Posts = pd.read_csv(r'C:\Users\user\PycharmProjects\dataprocessing2\Posts.csv')
Users = pd.read_csv(r'C:\Users\user\PycharmProjects\dataprocessing2\Users.csv.gz')
Comments = pd.read_csv(r'C:\Users\user\PycharmProjects\dataprocessing2\Comments.csv.gz')
Votes = pd.read_csv(r'C:\Users\user\PycharmProjects\dataprocessing2\Votes.csv.gz')
db = os.path.join(tempfile.mkdtemp(), "example.db")
if os.path.isfile(db):
    os.remove(db)
conn = sqlite3.connect(db) # create the connection
Badges.to_sql("Badges", conn) # import the data frame into the database
Comments.to_sql("Comments", conn)
Posts.to_sql("Posts", conn)
Users.to_sql("Users", conn)
Votes.to_sql("Votes", conn)
ref1 = pd.read_sql_query(
    """
        SELECT STRFTIME('%Y', CreationDate) AS Year, COUNT(*) AS TotalNumber
    FROM Posts
    GROUP BY Year

    """,conn)

ref2 = pd.read_sql_query(
    """
        SELECT Id, DisplayName, SUM(ViewCount) AS TotalViews
FROM Users
JOIN (
SELECT OwnerUserId, ViewCount FROM Posts WHERE PostTypeId = 1
) AS Questions
ON Users.Id = Questions.OwnerUserId
GROUP BY Id
ORDER BY TotalViews DESC
LIMIT 10

    """,conn)
ref3 = pd.read_sql_query(
    """
      SELECT Year, Name, MAX((Count * 1.0) / CountTotal) AS MaxPercentage
FROM (
SELECT BadgesNames.Year, BadgesNames.Name, BadgesNames.Count, BadgesYearly.CountTotal
FROM (
SELECT Name, COUNT(*) AS Count, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY Name, Year
) AS BadgesNames
JOIN (
SELECT COUNT(*) AS CountTotal, STRFTIME('%Y', Badges.Date) AS Year
FROM Badges
GROUP BY YEAR
) AS BadgesYearly
ON BadgesNames.Year = BadgesYearly.Year
)
GROUP BY Year

    """,conn)

ref4 = pd.read_sql_query(
    """
      SELECT Title, CommentCount, ViewCount, CommentsTotalScore, DisplayName, Reputation, Location
FROM (
SELECT Posts.OwnerUserId, Posts.Title, Posts.CommentCount, Posts.ViewCount,
CmtTotScr.CommentsTotalScore
FROM (
SELECT PostId, SUM(Score) AS CommentsTotalScore
FROM Comments
GROUP BY PostId
) AS CmtTotScr
JOIN Posts ON Posts.Id = CmtTotScr.PostId
WHERE Posts.PostTypeId=1
) AS PostsBestComments
JOIN Users ON PostsBestComments.OwnerUserId = Users.Id
ORDER BY CommentsTotalScore DESC
LIMIT 10

    """,conn)

ref5 = pd.read_sql_query(
    """
     SELECT Posts.Title, STRFTIME('%Y-%m-%d', Posts.CreationDate) AS Date, VotesByAge.*
FROM Posts
JOIN (
SELECT PostId,
MAX(CASE WHEN VoteDate = 'before' THEN Total ELSE 0 END) BeforeCOVIDVotes,
MAX(CASE WHEN VoteDate = 'during' THEN Total ELSE 0 END) DuringCOVIDVotes,
MAX(CASE WHEN VoteDate = 'after' THEN Total ELSE 0 END) AfterCOVIDVotes,
SUM(Total) AS Votes
FROM (
SELECT PostId,
CASE STRFTIME('%Y', CreationDate)
WHEN '2022' THEN 'after'
WHEN '2021' THEN 'during'
WHEN '2020' THEN 'during'
WHEN '2019' THEN 'during'
ELSE 'before'
END VoteDate, COUNT(*) AS Total
FROM Votes
WHERE VoteTypeId IN (3, 4, 12)
GROUP BY PostId, VoteDate
) AS VotesDates
GROUP BY VotesDates.PostId
) AS VotesByAge ON Posts.Id = VotesByAge.PostId
WHERE Title NOT IN ('') AND DuringCOVIDVotes > 0
ORDER BY DuringCOVIDVotes DESC, Votes DESC
LIMIT 20

    """,conn)


def are_equivalent(ref,res):
      ref= ref.sort_values(by=ref.columns.to_list()).reset_index(drop=True)
      res= res.sort_values(by=res.columns.to_list()).reset_index(drop=True)
      return ref.equals(res)







#############################################
res1 = Posts[["Id", "CreationDate"]]
res1["CreationDate"] = res1["CreationDate"].str[:4];
res1 = res1.groupby("CreationDate", as_index=False).agg(
    TotalNumber=('Id', "count")).rename(columns={"CreationDate": "Year"})
####################################################
Questions = Posts[Posts["PostTypeId"] == 1][["OwnerUserId", "ViewCount"]]
res2 = Users[["Id", "DisplayName"]].merge(Questions[["OwnerUserId", "ViewCount"]], left_on="Id",
                                          right_on="OwnerUserId").groupby("Id", as_index=False).agg(
    TotalViews=('ViewCount', "sum")).sort_values(by="TotalViews", ascending=False, ignore_index=True)[:10]
tmp2 = Users[["Id", "DisplayName"]].merge(Questions[["OwnerUserId", "ViewCount"]], left_on="Id",
                                          right_on="OwnerUserId").groupby("DisplayName", as_index=False).agg(
    TotalViews=('ViewCount', "sum")).sort_values(by="TotalViews", ascending=False, ignore_index=True)[:10]

res2 = res2[["Id", "TotalViews"]].merge(tmp2[["DisplayName", "TotalViews"]], left_on="TotalViews",
                                        right_on="TotalViews")
column_to_move = res2.pop("DisplayName")
res2.insert(1, "DisplayName", column_to_move)
###################################################
BadgesNames = Badges[["Name", "Id", "Date"]]
BadgesNames["Date"] = BadgesNames["Date"].str[:4]
BadgesNames = BadgesNames.groupby(["Name", "Date"]).agg(
    Count=('Id', "count"))

BadgesYearly = Badges[["Id", "Date"]]
BadgesYearly["Date"] = BadgesYearly["Date"].str[:4]
BadgesYearly = BadgesYearly.groupby(["Date"]).agg(
    CountTotal=('Id', "count"))
BigTable = BadgesYearly.join(BadgesNames, how="inner").reset_index().rename(columns={"Date": "Year"}).reset_index()[
    ["Year", "Name", "Count", "CountTotal"]]
BigTable["Count"] = 1.0 * BigTable["Count"] / BigTable["CountTotal"]
res3 = BigTable.loc[BigTable.groupby("Year")["Count"].idxmax()][["Year", "Name", "Count"]].reset_index(drop=True)
res3 = res3.rename(columns={"Count": "MaxPercentage"})
###################################################
CmtTotScr = Comments[["PostId", "Score"]].groupby("PostId").agg(CommentsTotalScore=("Score", "sum"))
PostsBestComments = Posts[Posts["PostTypeId"] == 1][["Id", "OwnerUserId", "Title", "CommentCount", "ViewCount"]]
PostsBestComments = PostsBestComments.merge(CmtTotScr, left_on='Id', right_on='PostId')
PostsBestComments = PostsBestComments[["OwnerUserId", "Title", "CommentCount", "ViewCount", "CommentsTotalScore"]]
res4 = PostsBestComments.merge(Users[["Id", "DisplayName", "Reputation", "Location"]], left_on="OwnerUserId",
                               right_on="Id").sort_values(by="CommentsTotalScore", ascending=False, ignore_index=True)[
       :10]
res4 = res4[["Title", "CommentCount", "ViewCount", "CommentsTotalScore", "DisplayName", "Reputation", "Location"]]
###################################################

votesDates = Votes[Votes["VoteTypeId"].isin([3, 4, 12])]
votesDates["CreationDate"] = np.where(votesDates['CreationDate'].str[:4] < '2019', 'before',
                                      np.where(votesDates['CreationDate'].str[:4] == '2022', 'after', 'during'))
votesDates = votesDates[["Id","PostId", "CreationDate"]].groupby(["PostId", "CreationDate"]).agg(Total=("Id", "count"))


VotesbyAge = votesDates.groupby(['PostId', 'CreationDate'])['Total'].max().unstack()
VotesbyAge = VotesbyAge.fillna(0)
VotesbyAge['Votes']=votesDates.groupby('PostId').agg(Votes=('Total','sum'))
Posts = Posts.set_index('Id')
res5=Posts[Posts['Title'].notnull()][['Title','CreationDate']].merge(VotesbyAge,left_on='Id',right_index=True)
res5 = res5[res5['during']>0]
res5=res5.reset_index()
res5=res5.rename(columns={"CreationDate": "Date", "Id":"PostId","during":"DuringCOVIDVotes","after":"AfterCOVIDVotes","before":"BeforeCOVIDVotes"})
res5=res5[["Title","Date","PostId","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes","Votes"]]
res5=res5.sort_values(by=["DuringCOVIDVotes","Votes"], ascending=[False,False])[:20]
res5=res5.reset_index()
res5=res5[["Title","Date","PostId","BeforeCOVIDVotes","DuringCOVIDVotes","AfterCOVIDVotes","Votes"]]
res5["Date"]=res5["Date"].str[:10]
if __name__ == '__main__':
    print(are_equivalent(ref1, res1))
    print(are_equivalent(ref2, res2))
    print(are_equivalent(ref3, res3))
    print(are_equivalent(ref4, res4))
    print(are_equivalent(ref5, res5))
    print(res5==ref5)



conn.close()
