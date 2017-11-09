/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 *  [2014] Elasticsearch Incorporated. All Rights Reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch Incorporated and its suppliers,
 * if any.  The intellectual and technical concepts contained
 * herein are proprietary to Elasticsearch Incorporated
 * and its suppliers and may be covered by U.S. and Foreign Patents,
 * patents in process, and are protected by trade secret or copyright law.
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from Elasticsearch Incorporated.
 */

/** Fork from Presto Parser - significantly trimmed down and adjusted for ES */
/** presto-parser/src/main/antlr4/com/facebook/presto/sql/parser/SqlBase.g4 grammar */

grammar SqlBase;

tokens {
    DELIMITER
}

singleStatement
    : statement EOF
    ;

singleExpression
    : expression EOF
    ;

statement
    : query                                                                                               #statementDefault
    | EXPLAIN
        ('('
        (
          PLAN type=(PARSED | ANALYZED | OPTIMIZED | MAPPED | EXECUTABLE | ALL)
        | FORMAT format=(TEXT | GRAPHVIZ)
        | VERIFY verify=booleanValue
        )*
        ')')?
        statement                                                                                         #explain
    | DEBUG
        ('('
        (
          PLAN type=(ANALYZED | OPTIMIZED)
        | FORMAT format=(TEXT | GRAPHVIZ)
        )*
        ')')?
        statement                                                                                         #debug
    | SHOW TABLES (LIKE? pattern=STRING)?                                                                 #showTables
    | SHOW COLUMNS (FROM | IN) tableIdentifier                                                            #showColumns
    | (DESCRIBE | DESC) tableIdentifier                                                                   #showColumns
    | SHOW FUNCTIONS (LIKE? pattern=STRING)?                                                              #showFunctions
    | SHOW SCHEMAS                                                                                        #showSchemas
    ;

query
    : (WITH namedQuery (',' namedQuery)*)? queryNoWith
    ;

queryNoWith
    : queryTerm
    /** we could add sort by - sort per partition */
      (ORDER BY orderBy (',' orderBy)*)?
      (LIMIT limit=(INTEGER_VALUE | ALL))?
    ;

queryTerm
    : querySpecification                   #queryPrimaryDefault
    | '(' queryNoWith  ')'                 #subquery
    ;

orderBy
    : expression ordering=(ASC | DESC)?
    ;

querySpecification
    : SELECT setQuantifier? selectItem (',' selectItem)*
      fromClause?
      (WHERE where=booleanExpression)?
      (GROUP BY groupBy)?
      (HAVING having=booleanExpression)?
    ;

fromClause
    : FROM relation (',' relation)*
    ;

groupBy
    : setQuantifier? groupingElement (',' groupingElement)*
    ;

groupingElement
    : groupingExpressions                         #singleGroupingSet
    ;

groupingExpressions
    : '(' (expression (',' expression)*)? ')'
    | expression
    ;

namedQuery
    : name=identifier AS '(' queryNoWith ')'
    ;

setQuantifier
    : DISTINCT
    | ALL
    ;

selectItem
    : expression (AS? identifier)?                #selectExpression
    ;

relation
    : relationPrimary joinRelation*
    ;

joinRelation
    : (joinType) JOIN right=relationPrimary joinCriteria?
    | NATURAL joinType JOIN right=relationPrimary
    ;

joinType
    : INNER?
    | LEFT OUTER?
    | RIGHT OUTER?
    | FULL OUTER?
    ;

joinCriteria
    : ON booleanExpression
    | USING '(' identifier (',' identifier)* ')'
    ;

relationPrimary
    : tableIdentifier (AS? qualifiedName)?                            #tableName
    | '(' queryNoWith ')' (AS? qualifiedName)?                        #aliasedQuery
    | '(' relation ')' (AS? qualifiedName)?                           #aliasedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                                                 #logicalNot
    | EXISTS '(' query ')'                                                                  #exists
    | QUERY '(' queryString=STRING (',' options=STRING)* ')'                                #stringQuery
    | MATCH '(' singleField=qualifiedName ',' queryString=STRING (',' options=STRING)* ')'  #matchQuery
    | MATCH '(' multiFields=STRING ',' queryString=STRING (',' options=STRING)* ')'         #multiMatchQuery
    | predicated                                                                            #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression                           #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                            #logicalBinary
    ;

// workaround for:
//  https://github.com/antlr/antlr4/issues/780
//  https://github.com/antlr/antlr4/issues/781
predicated
    : valueExpression predicate?
    ;

// dedicated calls for each branch are not used to reuse the NOT handling across them
// instead the property kind is used to differentiate
predicate
    : NOT? kind=BETWEEN lower=valueExpression AND upper=valueExpression
    | NOT? kind=IN '(' expression (',' expression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=(LIKE | RLIKE) pattern=valueExpression
    | IS NOT? kind=NULL
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                     #comparison
    ;

primaryExpression
    : CAST '(' expression AS dataType ')'                                            #cast
    | EXTRACT '(' field=identifier FROM valueExpression ')'                          #extract
    | constant                                                                       #constantDefault
    | ASTERISK                                                                       #star
    | (qualifier=columnExpression '.')? ASTERISK                                     #star
    | identifier '(' (setQuantifier? expression (',' expression)*)? ')'              #functionCall
    | '(' query ')'                                                                  #subqueryExpression
    | columnExpression                                                               #columnReference
    | base=columnExpression '.' fieldName=identifier                                 #dereference
    | '(' expression ')'                                                             #parenthesizedExpression
    ;

columnExpression
    : ((alias=identifier | table=tableIdentifier) '.' )? name=identifier
    ;

constant
    : NULL                                                                                     #nullLiteral
    | identifier STRING                                                                        #typeConstructor
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

dataType
    : identifier                                                                      #primitiveDataType
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

qualifiedName
    : identifier ('.' identifier)*
    ;

tableIdentifier
    : index=identifier
    ;

identifier
    : quoteIdentifier
    | unquoteIdentifier
    ;

quoteIdentifier
    : QUOTED_IDENTIFIER      #quotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

unquoteIdentifier
    : IDENTIFIER             #unquotedIdentifier
    | nonReserved            #unquotedIdentifier
    | DIGIT_IDENTIFIER       #digitIdentifier
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    ;

// http://developer.mimer.se/validator/sql-reserved-words.tml
nonReserved
    : ANALYZE | ANALYZED 
    | COLUMNS 
    | DEBUG 
    | EXECUTABLE | EXPLAIN 
    | FORMAT | FUNCTIONS | FROM 
    | GRAPHVIZ 
    | LOGICAL 
    | MAPPED 
    | OPTIMIZED 
    | PARSED | PHYSICAL | PLAN 
    | QUERY 
    | RESET | RLIKE 
    | SCHEMAS | SESSION | SETS | SHOW 
    | TABLES | TEXT |  TYPE 
    | USE 
    | VERIFY
    ;

ALL: 'ALL';
ANALYZE: 'ANALYZE';
ANALYZED: 'ANALYZED';
AND: 'AND';
ANY: 'ANY';
AS: 'AS';
ASC: 'ASC';
BETWEEN: 'BETWEEN';
BY: 'BY';
CAST: 'CAST';
COLUMN: 'COLUMN';
COLUMNS: 'COLUMNS';
CROSS: 'CROSS';
DEBUG: 'DEBUG';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DISTINCT: 'DISTINCT';
EXECUTABLE: 'EXECUTABLE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FOR: 'FOR';
FORMAT: 'FORMAT';
FROM: 'FROM';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
GROUPING: 'GROUPING';
HAVING: 'HAVING';
IN: 'IN';
INNER: 'INNER';
INTEGER: 'INTEGER';
INTO: 'INTO';
IS: 'IS';
JOIN: 'JOIN';
LAST: 'LAST';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
LOGICAL: 'LOGICAL';
MAPPED: 'MAPPED';
MATCH: 'MATCH';
NATURAL: 'NATURAL';
NO: 'NO';
NOT: 'NOT';
NULL: 'NULL';
ON: 'ON';
OPTIMIZED: 'OPTIMIZED';
OPTION: 'OPTION';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
PARSED: 'PARSED';
PHYSICAL: 'PHYSICAL';
PLAN: 'PLAN';
QUERY: 'QUERY';
RESET: 'RESET';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
SCHEMAS: 'SCHEMAS';
SELECT: 'SELECT';
SESSION: 'SESSION';
SET: 'SET';
SETS: 'SETS';
SHOW: 'SHOW';
TABLE: 'TABLE';
TABLES: 'TABLES';
TEXT: 'TEXT';
THEN: 'THEN';
TO: 'TO';
TRUE: 'TRUE';
TYPE: 'TYPE';
USE: 'USE';
USING: 'USING';
VERIFY: 'VERIFY';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';

EQ  : '=';
NEQ : '<>' | '!=' | '<=>';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CONCAT: '||';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    | DIGIT+ ('.' DIGIT*)? EXPONENT
    | '.' DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@' | ':')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~['"'|'.'] | '"' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~['`'|'.'] | '``' )* '`'
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ~[\r\n]* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
