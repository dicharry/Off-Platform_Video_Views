SELECT
  *,
  DATE_SUB(Month, INTERVAL 1 YEAR) AS LY,
  SUM(Video_Views) OVER (PARTITION BY SOURCE, Brand, Channel ORDER BY Month ROWS BETWEEN 12 PRECEDING AND 12 PRECEDING) AS LY_Value
FROM ( (
    SELECT
      'YouTube' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
        brand AS Brand,
        CASE
          WHEN channel_title__youtube = "Wall Street Journal" THEN "WSJ"
        ELSE
        channel_title__youtube
      END
        AS Channel,
        SUM(CAST(views__youtube AS int)) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM
        `X.X.X`
      WHERE
        Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
      GROUP BY
        Month,
        brand,
        channel_title__youtube ))
    --for WSJ, only returning videos with 10K or more plays. a difference of 18k rows saved by this.
    -- where brand != 'WSJ' OR (brand = 'WSJ' AND video_views > 10000)
  UNION ALL
    --organic_social_post_content and tweet_text__twitter_organic have the same counts and appear to look the same
    --organic_social_video_views and video_total_views__twitter_organic have the same values
    --video_content_starts__twitter_organic is much higher than the other two. need to get a definition on content start
    --the brands are the same as the channels, so just make a duplicate
    (
    SELECT
      'Twitter' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
        brand AS Brand,
        brand AS Channel,
        SUM(organic_social_video_views) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM
        `X.X.X`
      WHERE
        organic_social_video_views > 0
        AND Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
      GROUP BY
        Month,
        brand ))
  UNION ALL
    --have to combine two fields, video views and plays, because insta changed all videos to reels around Aug/Sep 2022.
    (
    SELECT
      'Instagram' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        CASE
          WHEN account_name__instagram_insights = "Barron's" THEN "Barron's"
        ELSE
        account_name__instagram_insights
      END
        AS Channel,
        brand AS Brand,
        SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
        SUM(plays__instagram_insights) AS reels_plays,
        -- sum(cast(plays__instagram_insights AS int)) as plays
        SUM(video_views__instagram_insights) AS video_views_from_feed,
        SUM(plays__instagram_insights) + SUM(video_views__instagram_insights) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM
        `X.X.X`
      WHERE
        Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
      GROUP BY
        Month,
        account_name__instagram_insights,
        brand )
    WHERE
      Video_Views > 0)
  UNION ALL
    --product and an_accounts are the same and they have the same results
    (
    SELECT
      'Apple News' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
        CASE
          WHEN product = 'WSJ Magazine' THEN 'WSJ'
          WHEN product = 'Barrons' THEN "Barron's"
        ELSE
        product
      END
        AS Brand,
        CASE
          WHEN product = 'Barrons' THEN "Barron's"
        ELSE
        product
      END
        AS Channel,
        SUM(video_views) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM
        `X.X.X`
      WHERE
        Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
      GROUP BY
        Month,
        product ))
  UNION ALL
    --total_video_views is the sum of both clicked_videos and auto_played_videos
    (
    SELECT
      'Facebook' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        Month,
        Channel,
        CASE
          WHEN Channel = 'WSJ Opinion ' THEN 'WSJ Opinion'
          WHEN Channel = 'The Wall Street Journal ' THEN 'WSJ'
          WHEN Channel = 'WSJ+ ' THEN 'WSJ'
          WHEN Channel = 'WSJ Magazine ' THEN 'WSJ'
        ELSE
        Channel
      END
        AS Brand,
        SUM(Video_Views) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM (
        SELECT
          SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
          brand AS Brand,
          REPLACE(data_source_name, "- FB Page Insights", "") AS Channel,
          SUM(Total_Video_Views__Facebook_Pages) AS Video_Views
        FROM
          `X.X.X`
        WHERE
          Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
        GROUP BY
          Month,
          brand,
          Channel )
      GROUP BY
        Month,
        Brand,
        Channel )
    WHERE
      Video_Views > 0
    ORDER BY
      Brand DESC)
  UNION ALL (
    SELECT
      'Snapchat' AS SOURCE,
      PARSE_DATE('%Y-%m', Month) AS Month,
      Brand,
      Channel,
      Video_Views,
      RANK() OVER (PARTITION BY last_month, Brand ORDER BY Video_Views DESC) AS Rank
    FROM (
      SELECT
        SUBSTR(FORMAT_DATE('%Y-%m', Date), 0, 10) AS Month,
        CASE
          WHEN Publisher__Snapchat_Publisher = 'MarketWatch Show' THEN 'MarketWatch'
          WHEN Publisher__Snapchat_Publisher = 'MarketWatch Dynamic Stories' THEN 'MarketWatch'
          WHEN Publisher__Snapchat_Publisher = 'Mansion Global Dynamic Stories' THEN 'Mansion Global'
          WHEN Publisher__Snapchat_Publisher = 'Wall Street Journal' THEN 'WSJ'
          WHEN Publisher__Snapchat_Publisher = 'WSJ Opinion' THEN 'WSJ'
          WHEN Publisher__Snapchat_Publisher = "Investor's Business Daily Dynamic Stories" THEN 'IBD'
          WHEN Publisher__Snapchat_Publisher = "Barron's Dynamic Stories" THEN "Barron's"
        ELSE
        Publisher__Snapchat_Publisher
      END
        AS Brand,
        CASE
          WHEN Publisher__Snapchat_Publisher = 'MarketWatch Show' THEN 'MarketWatch Show'
          WHEN Publisher__Snapchat_Publisher = 'MarketWatch Dynamic Stories' THEN 'MarketWatch Stories'
          WHEN Publisher__Snapchat_Publisher = 'Mansion Global Dynamic Stories' THEN 'Mansion Global Stories'
          WHEN Publisher__Snapchat_Publisher = 'Wall Street Journal' THEN 'WSJ Stories'
          WHEN Publisher__Snapchat_Publisher = 'WSJ Opinion' THEN 'Future View Show'
          WHEN Publisher__Snapchat_Publisher = "Investor's Business Daily Dynamic Stories" THEN 'IBD Stories'
          WHEN Publisher__Snapchat_Publisher = "Barron's Dynamic Stories" THEN "Barron's Stories"
        ELSE
        Publisher__Snapchat_Publisher
      END
        AS Channel,
        SUM(Daily_Behavior___Total_views_Topsnap__Snapchat_Publisher) AS Video_Views,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) AS last_month
      FROM
        `X.X.X`
      WHERE
        Date > DATE_SUB(CURRENT_DATE(), INTERVAL 26 MONTH)
        AND Publisher__Snapchat_Publisher IN ('MarketWatch Show',
          'WSJ Opinion') #only looking in these channels, because they are the only channels that have video views currently. video views on the dynamic stories channels happen after a swipe-up, which would be in the Behavior report as "Video views (Attachment)", but we should also see these video views in O&O data
      GROUP BY
        Month,
        Brand,
        Channel )) )a
  -- LEFT JOIN
  -- (SELECT * FROM `dj-users.dicharryd.AV Dash - Video Channel Level`) b
  -- USING (Source, Brand, Channel)
  -- WHERE
  -- b.Month = DATE_TRUNC(DATE_SUB(a.Month, INTERVAL 12 MONTH), month)
ORDER BY SOURCE,
  Brand,
  Channel,
  Month