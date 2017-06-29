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
    | SHOW TABLES ((FROM | IN) index=identifier)? (LIKE? pattern=STRING)?                                 #showTables
    | SHOW COLUMNS (FROM | IN) tableIdentifier                                                            #showColumns
    | (DESCRIBE | DESC) tableIdentifier                                                                   #showColumns
    | SHOW FUNCTIONS (LIKE? pattern=STRING)?                                                              #showFunctions
    | SHOW SCHEMAS                                                                                        #showSchemas
    | SHOW SESSION (key=identifier | (LIKE? pattern=STRING) | ALL)                                        #showSession
    | SET SESSION? key=identifier EQ? value=constant                                                      #sessionSet
    | RESET SESSION? (key=identifier | (LIKE? pattern=STRING) | ALL)                                      #sessionReset
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
    : predicated                                                                            #booleanDefault
    | NOT booleanExpression                                                                 #logicalNot
    | left=booleanExpression operator=AND right=booleanExpression                           #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                            #logicalBinary
    | EXISTS '(' query ')'                                                                  #exists
    | QUERY '(' queryString=STRING (',' options=STRING)* ')'                                #stringQuery
    | MATCH '(' singleField=qualifiedName ',' queryString=STRING (',' options=STRING)* ')'  #matchQuery
    | MATCH '(' multiFields=STRING ',' queryString=STRING (',' options=STRING)* ')'         #multiMatchQuery
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
    : constant                                                                       #constantDefault
    | ASTERISK                                                                       #star
    | (qualifier=columnExpression '.')? ASTERISK                                     #star
    | identifier '(' (setQuantifier? expression (',' expression)*)? ')'              #functionCall
    | '(' query ')'                                                                  #subqueryExpression
    | columnExpression                                                               #columnReference
    | base=columnExpression '.' fieldName=identifier                                 #dereference
    | '(' expression ')'                                                             #parenthesizedExpression
    | CAST '(' expression AS dataType ')'                                            #cast
    | EXTRACT '(' field=identifier FROM valueExpression ')'                          #extract
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
    : index=identifier ('.' type=identifier)?
    | '"' uindex=unquoteIdentifier ('.' utype=unquoteIdentifier)? '"'
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
    : SHOW | TABLES | COLUMNS | COLUMN | FUNCTIONS
    | EXPLAIN | ANALYZE | FORMAT | TYPE | TEXT | GRAPHVIZ | LOGICAL | PHYSICAL | VERIFY
    ;

SELECT: 'SELECT';
FROM: 'FROM';
AS: 'AS';
ALL: 'ALL';
WHEN: 'WHEN';
THEN: 'THEN';
ANY: 'ANY';
DISTINCT: 'DISTINCT';
WHERE: 'WHERE';
GROUP: 'GROUP';
BY: 'BY';
GROUPING: 'GROUPING';
SETS: 'SETS';
ORDER: 'ORDER';
HAVING: 'HAVING';
LIMIT: 'LIMIT';
OR: 'OR';
AND: 'AND';
IN: 'IN';
NOT: 'NOT';
NO: 'NO';
EXISTS: 'EXISTS';
BETWEEN: 'BETWEEN';
LIKE: 'LIKE';
RLIKE: 'RLIKE';
IS: 'IS';
NULL: 'NULL';
TRUE: 'TRUE';
FALSE: 'FALSE';
LAST: 'LAST';
ASC: 'ASC';
DESC: 'DESC';
FOR: 'FOR';
INTEGER: 'INTEGER';
JOIN: 'JOIN';
CROSS: 'CROSS';
OUTER: 'OUTER';
INNER: 'INNER';
LEFT: 'LEFT';
RIGHT: 'RIGHT';
FULL: 'FULL';
NATURAL: 'NATURAL';
USING: 'USING';
ON: 'ON';
WITH: 'WITH';
TABLE: 'TABLE';
INTO: 'INTO';
DESCRIBE: 'DESCRIBE';
OPTION: 'OPTION';
EXPLAIN: 'EXPLAIN';
ANALYZE: 'ANALYZE';
FORMAT: 'FORMAT';
TYPE: 'TYPE';
TEXT: 'TEXT';
VERIFY: 'VERIFY';
GRAPHVIZ: 'GRAPHVIZ';
LOGICAL: 'LOGICAL';
PHYSICAL: 'PHYSICAL';
SHOW: 'SHOW';
TABLES: 'TABLES';
COLUMNS: 'COLUMNS';
COLUMN: 'COLUMN';
FUNCTIONS: 'FUNCTIONS';
TO: 'TO';
DEBUG: 'DEBUG';
PLAN: 'PLAN';
PARSED: 'PARSED';
ANALYZED: 'ANALYZED';
OPTIMIZED: 'OPTIMIZED';
MAPPED: 'MAPPED';
EXECUTABLE: 'EXECUTABLE';
USE: 'USE';
SET: 'SET';
RESET: 'RESET';
SESSION: 'SESSION';
SCHEMAS: 'SCHEMAS';
EXTRACT: 'EXTRACT';
QUERY: 'QUERY';
MATCH: 'MATCH';
CAST: 'CAST';

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
