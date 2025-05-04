import { PrismaClient } from '@prisma/client'
import fs from 'fs'
import lodash from 'lodash'

import { config } from 'dotenv'
import path from 'path';

config();


const prisma = new PrismaClient()

function problem1() {
  return prisma.$queryRaw`
    SELECT    firstName,
              lastName,
              income
    FROM      Customer
    
    WHERE     income        BETWEEN 50000 AND 60000

    ORDER BY  income        DESC,
              lastName      ASC,
              firstName     ASC
    LIMIT     10;
  `
}

// 뭔가 BigInt와 관련된 문제때문에 json에 diff를 찍어보면 똑같은데 채점은 Incorrect로 나옵니다
function problem2() {
  return prisma.$queryRaw`
    SELECT    e.sin         sin,
              b.branchName  branchName,
              e.salary      salary,
              (mgr.salary - e.salary) "Salary Diff"
    FROM      Employee      e

    JOIN      Branch b      ON b.branchNumber = e.branchNumber
    JOIN      Employee mgr  ON mgr.sin = b.managerSIN

    WHERE     b.branchName  IN ('London', 'Berlin')

    ORDER BY  (mgr.salary - e.salary) DESC
    LIMIT     10;
  `
}

function problem3() {
  return prisma.$queryRaw`
    SELECT    c1.firstName  firstName,
              c1.lastName   lastName,
              c1.income     income
    FROM      Customer      c1
    
    LEFT JOIN Customer c2   ON c2.lastName = 'Butler'

    GROUP BY  c1.customerID
    HAVING    c1.income >= 2 * MAX(c2.income)

    ORDER BY  c1.lastName   ASC,
              c1.firstName  ASC
    LIMIT     10;
  `
}

function problem4() {
  return prisma.$queryRaw`
    SELECT    DISTINCT
              c.customerID,
              c.income,
              aAl.accNumber,
              aAl.branchNumber
    FROM      Customer c
    
    JOIN      Owns oL       ON oL.customerID = c.customerID
    JOIN      Account aL    ON aL.accNumber = oL.accNumber
    JOIN      Branch bL     ON bL.branchNumber = aL.branchNumber
                            AND bL.branchName = 'London'
    JOIN      Owns oT       ON oT.customerID = c.customerID
    JOIN      Account aT    ON aT.accNumber = oT.accNumber
    JOIN      Branch bT     ON bT.branchNumber = aT.branchNumber
                            AND bT.branchName = 'Latveria'
    JOIN      Owns oAl      ON oAl.customerID = c.customerID
    JOIN      Account aAl   ON aAl.accNumber = oAl.accNumber

    WHERE     c.income > 80000
    
    ORDER BY  c.customerID  ASC,
              aAl.accNumber ASC
    LIMIT     10;
  `
}

function problem5() {
  return prisma.$queryRaw`
    SELECT    DISTINCT
              o.customerID,
              a.type,
              a.accNumber,
              a.balance
    FROM      Owns o

    JOIN      Account a     ON a.accNumber = o.accNumber

    WHERE     a.type        IN ('BUS', 'SAV')

    ORDER BY  o.customerID  ASC,
              a.type        ASC,
              a.accNumber   ASC
    LIMIT     10;
  `
}

function problem6() {
  return prisma.$queryRaw`
    SELECT    b.branchName,
              a.accNumber,
              a.balance
    FROM      Branch b

    JOIN      Employee m    ON m.sin = b.managerSIN
                            AND m.firstName = 'Phillip'
                            AND m.lastName = 'Edwards'
    JOIN      Account a     ON a.branchNumber = b.branchNumber

    WHERE     CAST(a.balance AS SIGNED) > 100000

    ORDER BY  a.accNumber   ASC
    LIMIT     10;
  `
}

function problem7() {
  return prisma.$queryRaw`
    SELECT    c.customerID
    FROM      Customer c

    JOIN      Owns oNY      ON oNY.customerID = c.customerID
    JOIN      Account aNY   ON aNY.accNumber = oNY.accNumber
    JOIN      Branch bNY    ON bNY.branchNumber = aNY.branchNumber
                            AND bNY.branchName = 'New York'
    LEFT JOIN Owns oAl      ON oAl.customerID = c.customerID
    LEFT JOIN Owns oCo      ON oCo.accNumber = oAl.accNumber
    LEFT JOIN Owns oCL      ON oCL.customerID = oCo.customerID
    LEFT JOIN Account aCL   ON aCL.accNumber = oCL.accNumber
    LEFT JOIN Branch bCL    ON bCL.branchNumber = aCL.branchNumber
                            AND bCL.branchName = 'London'

    GROUP BY  c.customerID
    HAVING    COUNT(bCL.branchName) = 0
    
    ORDER BY  c.customerID  ASC
    LIMIT     10;
  `
}

function problem8() {
  return prisma.$queryRaw`
    SELECT    e.sin,
              e.firstName,
              e.lastName,
              e.salary,
              b.branchName
    FROM      Employee e

    LEFT JOIN Branch b      ON b.managerSIN = e.sin

    WHERE     e.salary > 50000

    ORDER BY  b.branchName  DESC,
              e.firstName   ASC
    LIMIT     10;
  `
}

function problem9() {
  return prisma.$queryRaw`
    SELECT    e.sin,
              e.firstName,
              e.lastName,
              e.salary,
              MAX(CASE WHEN e.sin = b.managerSIN THEN b.branchName END) AS branchName
    FROM      Employee e,
              Branch b

    WHERE     e.salary > 50000
    
    GROUP BY  e.sin

    ORDER BY  branchName    DESC,
              e.firstName   ASC
    LIMIT     10;
  `
}

function problem10() {
  return prisma.$queryRaw`
    SELECT    c.customerID,
              c.firstName,
              c.lastName,
              c.income
    FROM      Customer c

    JOIN      Owns oC       ON oC.customerID = c.customerID
    JOIN      Account aC    ON aC.accNumber = oC.accNumber
    JOIN      Customer h    ON h.firstName = 'Helen'
                            AND h.lastName = 'Morgan'
    JOIN      Owns oH       ON oH.customerID = h.customerID
    JOIN      Account aH    ON aH.accNumber = oH.accNumber

    WHERE     (c.income > 5000 OR (c.firstName = 'Helen' AND c.lastName = 'Morgan'))

    GROUP BY  c.customerID
    HAVING    COUNT(DISTINCT CASE
                WHEN aC.branchNumber = aH.branchNumber
                THEN aC.branchNumber END) = COUNT(DISTINCT aH.branchNumber)

    ORDER BY  c.income      DESC
    LIMIT     10;
  `;
}

function problem11() {
  return prisma.$queryRaw`
    SELECT    e1.sin,
              e1.firstName,
              e1.lastName,
              e1.salary
    FROM      Employee e1

    JOIN      Branch b1     ON b1.branchNumber = e1.branchNumber
                            AND b1.branchName = 'Berlin'
    LEFT JOIN Employee e2   ON e2.branchNumber = e1.branchNumber
                            AND e2.salary < e1.salary

    WHERE     e2.sin        IS NULL

    ORDER BY  e1.sin        ASC
    LIMIT     10;
  `;
}

// 뭔가 BigInt와 관련된 문제때문에 json에 diff를 찍어보면 똑같은데 채점은 Incorrect로 나옵니다
function problem14() {
  return prisma.$queryRaw`
    SELECT    SUM(e.salary) totalSalary
    FROM      Employee e

    JOIN      Branch   b    ON b.branchNumber = e.branchNumber
                            AND b.branchName = 'Moscow'

    LIMIT     10;
  `
}

function problem15() {
  return prisma.$queryRaw`
    SELECT    c.customerID,
              c.firstName,
              c.lastName
    FROM      Customer c

    JOIN      Owns o        ON o.customerID = c.customerID
    JOIN      Account a     ON a.accNumber = o.accNumber

    GROUP BY  c.customerID
    HAVING    COUNT(DISTINCT a.branchNumber) = 4

    ORDER BY  c.lastName    ASC,
              c.firstName   ASC
    LIMIT     10;
  `
}

function problem17() {
  return prisma.$queryRaw`
    SELECT    c.customerID,
              c.firstName,
              c.lastName,
              c.income,
              AVG(a.balance) AS "average account balance"
    FROM      Customer c

    JOIN      Owns o        ON o.customerID = c.customerID
    JOIN      Account a     ON a.accNumber  = o.accNumber

    WHERE     c.lastName    LIKE 'S%e%'

    GROUP BY  c.customerID,
              c.firstName,
              c.lastName,
              c.income
    HAVING    COUNT(DISTINCT o.accNumber) >= 3

    ORDER BY  c.customerID  ASC
    LIMIT     10;
  `;
}

function problem18() {
  return prisma.$queryRaw`
    SELECT    a.accNumber,
              a.balance,
              sum(t.amount) as "sum of transaction amounts"
    FROM      Account a

    JOIN      Branch b      ON b.branchNumber = a.branchNumber
                            AND b.branchName = 'Berlin'
    JOIN      Transactions t ON a.accNumber = t.accNumber

    GROUP BY  a.accNumber,
              a.balance
    HAVING    COUNT(t.transNumber) >= 10
    
    ORDER BY  sum(t.amount) ASC
    LIMIT     10;
  `;
}


const ProblemList = [
  problem1, problem2, problem3, problem4, problem5, problem6, problem7, problem8, problem9, problem10,
  problem11, problem14, problem15, problem17, problem18
]


async function main() {
  for (let i = 0; i < ProblemList.length; i++) {
    const result = await ProblemList[i]()
    const answer =  JSON.parse(fs.readFileSync(`${ProblemList[i].name}.json`, 'utf-8'));
    lodash.isEqual(result, answer) ? console.log(`${ProblemList[i].name}: Correct`) : console.log(`${ProblemList[i].name}: Incorrect`)
  }
}

main()
  .then(async () => {
    await prisma.$disconnect()
  })
  .catch(async (e) => {
    console.error(e)
    await prisma.$disconnect()
    process.exit(1)
  })