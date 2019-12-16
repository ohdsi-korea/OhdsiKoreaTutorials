Tutorial-Achitecture
============

AWS Achitecture for tutorial.

## ATLAS

- Stack 총 8개 구성
- 각 Stack별 m5.large 구성

## R-STUDIO

- Stack 총 8개 구성
- 각 Stack별 r5.16xlarge 구성

## Redshift

- Stack 총 8개 구성
- 각 Stack별 dc2.large Node 6개로 구성

## RDS(postgreSQL)

- Stack 총 8개 구성
- 각 Stack별 Aurora PostgreSQL db.t3.medium 구성

## MSSQL

- Auto Scalling 그룹 생성 최대 60개 생성
- Instance: OHDSI-VOCA-Tutorial (msad.4xlarge)
- MSSQL(1433)/RDP(3389) Port 허용

## Firewall Open

- IP대역: 123.142.157.0 (곤지암 현장)