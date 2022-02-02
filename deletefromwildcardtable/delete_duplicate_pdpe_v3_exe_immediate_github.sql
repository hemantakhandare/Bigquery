begin

---Added on 18-Oct-21 Hemant to DELETE duplicate record based on LAST_PROCESSED_ON. ie deleting older version of record
DECLARE f ARRAY<STRING> DEFAULT ['P001_P003','P004_P006','P007_P008','P009','P010_P011','P012_P015','P016_P019','P020_P026','P027_P030','P031_P035','P036_P060','P061_MORE'];
declare s1,s2 string;
declare n INT64;
set n=ARRAY_LENGTH(f);

--------------------------------- taking backup of duplicate entries------------------------------
INSERT INTO
  datasetx.emp_dups (_partitiontime,
    TABLE_ID,
    pt,
    CLIENT,
    e_num,
    VKORG,
    VTWEG,
    SPART,
    MAT,
    CATLG_PRICED_AT_e_num,
    RTL_PRICED_AT_e_num,
    SOURCE_DIVISION,
    PRICE_DIVISION,
    PRIMARY_BU,
    WERKS,
    WIN_CONTRACT,
    HIERCY,
    PREFR,
    PCTYP,
    CN_SEG_GROUP,
    NON_CN_SEG_GROUP,
    SELL_PRICE_STRAT,
    COGS_STRAT,
    CATALOG_PRICE,
    RTL_PRICE_ZVSR,
    RTL_SELL_UNT_PRICE_ZVSU,
    CONTRACT_PRICE_ZVBS,
    AFTR_RBT_PRICE_ZVBR,
    CMPR_ZVB0,
    NET_CONTRACT_PRICE,
    BASE_PRICE_WORKE,
    EXCISE_TAX_ZTF0,
    WAC_KZWI1,
    AWP_ZREF,
    MSRP_ZMSR,
    PSTYV,
    WRPST,
    WRPRPS,
    WRPCALCTYP,
    KBETR,
    INSTANT_RBT_ZVF0,
    CONSIGNMENT_MKUP_ZDP6,
    REBATE_ELIGIBLE_FLG,
    OE_REP_FLAG,
    EXT_PLANT,
    CREATED_ON,
    CREATED_BY,
    CHANGED_ON,
    CHANGED_BY,
    ATTR_INGST_TIMESTAMP,
    ATTR_INGST_SEQ,
    ATTR_INGST_OPR,
    ATTR_INGST_USER,
    ATTR_SRC_READ_STMP,
    LAST_PROCESSED_ON,
    LAST_CATLG_PRICE_CHG_STP,
    WIN_CONTRACT_BGRP,
    WIN_CONTRACT_VNDR_REF )
SELECT
  TIMESTAMP_TRUNC(TIMESTAMP(CURRENT_DATE()),DAY) AS pt,
  a.* EXCEPT (row_num)
FROM (
  SELECT
    _TABLE_SUFFIX TABLE_ID,
    _partitiontime pt,
    emp.*,
    ROW_NUMBER() OVER (PARTITION BY emp.CLIENT, emp.e_num, emp.VKORG, emp.VTWEG, emp.SPART, emp.MAT ORDER BY LAST_PROCESSED_ON DESC ) AS row_num
  FROM
    `datasetx.employee_*` AS emp
  WHERE
    _TABLE_SUFFIX IN ('P001_P003',
      'P004_P006',
      'P007_P008',
      'P009',
      'P010_P011',
      'P012_P015',
      'P016_P019',
      'P020_P026',
      'P027_P030',
      'P031_P035',
      'P036_P060',
      'P061_MORE') ) a
WHERE
  row_num>1 ;
  
  -------------------------------end-------------------------

WHILE n > 0 DO
 set s1=  f[SAFE_ORDINAL(n)];
 set s2=s1;
 -- select s1;
 
 EXECUTE IMMEDIATE format("""
  DELETE
FROM
  datasetx.employee_%s AS B
WHERE
  EXISTS (
  SELECT
    1
  FROM
    datasetx.emp_dups AS A
  WHERE
    A.e_num=B.e_num
    AND A.MAT=B.MAT
    AND A.CLIENT=B.CLIENT
    AND A.VKORG=B.VKORG
    AND A.SPART=B.SPART
    AND A.VTWEG=B.VTWEG
    AND A.TABLE_ID='%s'
    AND A.LAST_PROCESSED_ON=B.LAST_PROCESSED_ON
    AND A.ATTR_INGST_TIMESTAMP=B.ATTR_INGST_TIMESTAMP	
	AND A._PARTITIONTIME =TIMESTAMP_TRUNC(TIMESTAMP(current_date()),DAY)
	)
""", s1,s2);

    SET n = n - 1;
END WHILE;

end;

