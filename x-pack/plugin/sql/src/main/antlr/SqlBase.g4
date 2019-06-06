/*
 *  [2017] Elasticsearch Incorporated. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** Fork of Facebook Presto Parser - significantly trimmed down and adjusted for ES */
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
    | SHOW TABLES (INCLUDE FROZEN)? (tableLike=likePattern | tableIdent=tableIdentifier)?                 #showTables
    | SHOW COLUMNS (INCLUDE FROZEN)? (FROM | IN) (tableLike=likePattern | tableIdent=tableIdentifier)     #showColumns
    | (DESCRIBE | DESC) (INCLUDE FROZEN)? (tableLike=likePattern | tableIdent=tableIdentifier)            #showColumns
    | SHOW FUNCTIONS (likePattern)?                                                                       #showFunctions
    | SHOW SCHEMAS                                                                                        #showSchemas
    | SYS TABLES (CATALOG clusterLike=likePattern)?
                 (tableLike=likePattern | tableIdent=tableIdentifier)?
                 (TYPE string (',' string)* )?                                                            #sysTables
    | SYS COLUMNS (CATALOG cluster=string)?
                  (TABLE tableLike=likePattern | tableIdent=tableIdentifier)?
                  (columnPattern=likePattern)?                                                            #sysColumns
    | SYS TYPES ((PLUS | MINUS)?  type=number)?                                                           #sysTypes
    ;
    
query
    : (WITH namedQuery (',' namedQuery)*)? queryNoWith
    ;

queryNoWith
    : queryTerm
    /** we could add sort by - sort per partition */
      (ORDER BY orderBy (',' orderBy)*)?
      limitClause?
    ;

limitClause
    : LIMIT limit=(INTEGER_VALUE | ALL)                                                                   
    | LIMIT_ESC limit=(INTEGER_VALUE | ALL) ESC_END                                              
    ;
    
queryTerm
    : querySpecification                   #queryPrimaryDefault
    | '(' queryNoWith  ')'                 #subquery
    ;

orderBy
    : expression ordering=(ASC | DESC)? (NULLS nullOrdering=(FIRST | LAST))?
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
    : FROZEN? tableIdentifier (AS? qualifiedName)?                    #tableName
    | '(' queryNoWith ')' (AS? qualifiedName)?                        #aliasedQuery
    | '(' relation ')' (AS? qualifiedName)?                           #aliasedRelation
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : NOT booleanExpression                                                                 #logicalNot
    | EXISTS '(' query ')'                                                                  #exists
    | QUERY '(' queryString=string matchQueryOptions ')'                                    #stringQuery
    | MATCH '(' singleField=qualifiedName ',' queryString=string matchQueryOptions ')'      #matchQuery
    | MATCH '(' multiFields=string ',' queryString=string matchQueryOptions ')'             #multiMatchQuery
    | predicated                                                                            #booleanDefault
    | left=booleanExpression operator=AND right=booleanExpression                           #logicalBinary
    | left=booleanExpression operator=OR right=booleanExpression                            #logicalBinary
    ;

matchQueryOptions
    : (',' string)*
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
    | NOT? kind=IN '(' valueExpression (',' valueExpression)* ')'
    | NOT? kind=IN '(' query ')'
    | NOT? kind=LIKE pattern
    | NOT? kind=RLIKE regex=string
    | IS NOT? kind=NULL
    ;

likePattern
    : LIKE pattern
    ;
    
pattern
    : value=string patternEscape?
    ;
    
patternEscape
    : ESCAPE escape=string
    | ESCAPE_ESC escape=string '}'
    ;

valueExpression
    : primaryExpression                                                                 #valueExpressionDefault
    | operator=(MINUS | PLUS) valueExpression                                           #arithmeticUnary
    | left=valueExpression operator=(ASTERISK | SLASH | PERCENT) right=valueExpression  #arithmeticBinary
    | left=valueExpression operator=(PLUS | MINUS) right=valueExpression                #arithmeticBinary
    | left=valueExpression comparisonOperator right=valueExpression                     #comparison
    ;

primaryExpression
    : castExpression                                                                           #cast
    | primaryExpression CAST_OP dataType                                                       #castOperatorExpression
    | extractExpression                                                                        #extract
    | builtinDateTimeFunction                                                                  #currentDateTimeFunction
    | constant                                                                                 #constantDefault
    | (qualifiedName DOT)? ASTERISK                                                            #star
    | functionExpression                                                                       #function
    | '(' query ')'                                                                            #subqueryExpression
    | qualifiedName                                                                            #dereference
    | '(' expression ')'                                                                       #parenthesizedExpression
    | CASE (operand=booleanExpression)? whenClause+ (ELSE elseClause=booleanExpression)? END   #case
    ;

builtinDateTimeFunction
    : name=CURRENT_TIMESTAMP
    | name=CURRENT_DATE
    | name=CURRENT_TIME
    ;

castExpression
    : castTemplate
    | FUNCTION_ESC castTemplate ESC_END
    | convertTemplate
    | FUNCTION_ESC convertTemplate ESC_END
    ;

castTemplate
    : CAST '(' expression AS dataType ')'
    ;

convertTemplate
    : CONVERT '(' expression ',' dataType ')'
    ;

extractExpression
    : extractTemplate
    | FUNCTION_ESC extractTemplate ESC_END
    ;
    
extractTemplate
    : EXTRACT '(' field=identifier FROM valueExpression ')'
    ;

functionExpression
    : functionTemplate
    | FUNCTION_ESC functionTemplate '}'
    ;
    
functionTemplate
    : functionName '(' (setQuantifier? expression (',' expression)*)? ')'
    ;
functionName
    : LEFT 
    | RIGHT 
    | identifier
    ;
    
constant
    : NULL                                                                                     #nullLiteral
    | interval                                                                                 #intervalLiteral
    | number                                                                                   #numericLiteral
    | booleanValue                                                                             #booleanLiteral
    | STRING+                                                                                  #stringLiteral
    | PARAM                                                                                    #paramLiteral
    | DATE_ESC string ESC_END                                                                  #dateEscapedLiteral
    | TIME_ESC string ESC_END                                                                  #timeEscapedLiteral
    | TIMESTAMP_ESC string ESC_END                                                             #timestampEscapedLiteral
    | GUID_ESC string ESC_END                                                                  #guidEscapedLiteral
    ;

comparisonOperator
    : EQ | NULLEQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

interval
    : INTERVAL sign=(PLUS | MINUS)? (valueNumeric=number | valuePattern=string) leading=intervalField (TO trailing=intervalField)? 
    ;
    
intervalField
    : YEAR | YEARS | MONTH | MONTHS | DAY | DAYS | HOUR | HOURS | MINUTE | MINUTES | SECOND | SECONDS
    ;

dataType
    : identifier                                                                      #primitiveDataType
    ;

qualifiedName
    : (identifier DOT)* identifier
    ;

identifier
    : quoteIdentifier
    | unquoteIdentifier
    ;

tableIdentifier
    : (catalog=identifier ':')? TABLE_IDENTIFIER
    | (catalog=identifier ':')? name=identifier
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

string
    : PARAM
    | STRING
    ;

whenClause
    : WHEN condition=expression THEN result=expression
    ;

// http://developer.mimer.se/validator/sql-reserved-words.tml
nonReserved
    : ANALYZE | ANALYZED 
    | CATALOGS | COLUMNS | CURRENT_DATE | CURRENT_TIME | CURRENT_TIMESTAMP
    | DAY | DEBUG  
    | EXECUTABLE | EXPLAIN 
    | FIRST | FORMAT | FULL | FUNCTIONS
    | GRAPHVIZ
    | HOUR
    | INTERVAL
    | LAST | LIMIT 
    | MAPPED | MINUTE | MONTH
    | OPTIMIZED 
    | PARSED | PHYSICAL | PLAN 
    | QUERY 
    | RLIKE
    | SCHEMAS | SECOND | SHOW | SYS
    | TABLES | TEXT | TYPE | TYPES
    | VERIFY
    | YEAR
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
CASE: 'CASE';
CAST: 'CAST';
CATALOG: 'CATALOG';
CATALOGS: 'CATALOGS';
COLUMNS: 'COLUMNS';
CONVERT: 'CONVERT';
CURRENT_DATE : 'CURRENT_DATE';
CURRENT_TIME : 'CURRENT_TIME';
CURRENT_TIMESTAMP : 'CURRENT_TIMESTAMP';
DAY: 'DAY';
DAYS: 'DAYS';
DEBUG: 'DEBUG';
DESC: 'DESC';
DESCRIBE: 'DESCRIBE';
DISTINCT: 'DISTINCT';
ELSE: 'ELSE';
END: 'END';
ESCAPE: 'ESCAPE';
EXECUTABLE: 'EXECUTABLE';
EXISTS: 'EXISTS';
EXPLAIN: 'EXPLAIN';
EXTRACT: 'EXTRACT';
FALSE: 'FALSE';
FIRST: 'FIRST';
FORMAT: 'FORMAT';
FROM: 'FROM';
FROZEN: 'FROZEN';
FULL: 'FULL';
FUNCTIONS: 'FUNCTIONS';
GRAPHVIZ: 'GRAPHVIZ';
GROUP: 'GROUP';
HAVING: 'HAVING';
HOUR: 'HOUR';
HOURS: 'HOURS';
IN: 'IN';
INCLUDE: 'INCLUDE';
INNER: 'INNER';
INTERVAL: 'INTERVAL';
IS: 'IS';
JOIN: 'JOIN';
LAST: 'LAST';
LEFT: 'LEFT';
LIKE: 'LIKE';
LIMIT: 'LIMIT';
MAPPED: 'MAPPED';
MATCH: 'MATCH';
MINUTE: 'MINUTE';
MINUTES: 'MINUTES';
MONTH: 'MONTH';
MONTHS: 'MONTHS';
NATURAL: 'NATURAL';
NOT: 'NOT';
NULL: 'NULL';
NULLS: 'NULLS';
ON: 'ON';
OPTIMIZED: 'OPTIMIZED';
OR: 'OR';
ORDER: 'ORDER';
OUTER: 'OUTER';
PARSED: 'PARSED';
PHYSICAL: 'PHYSICAL';
PLAN: 'PLAN';
RIGHT: 'RIGHT';
RLIKE: 'RLIKE';
QUERY: 'QUERY';
SCHEMAS: 'SCHEMAS';
SECOND: 'SECOND';
SECONDS: 'SECONDS';
SELECT: 'SELECT';
SHOW: 'SHOW';
SYS: 'SYS';
TABLE: 'TABLE';
TABLES: 'TABLES';
TEXT: 'TEXT';
THEN: 'THEN';
TRUE: 'TRUE';
TO: 'TO';
TYPE: 'TYPE';
TYPES: 'TYPES';
USING: 'USING';
VERIFY: 'VERIFY';
WHEN: 'WHEN';
WHERE: 'WHERE';
WITH: 'WITH';
YEAR: 'YEAR';
YEARS: 'YEARS';

// Escaped Sequence
ESCAPE_ESC: '{ESCAPE';
FUNCTION_ESC: '{FN';
LIMIT_ESC:'{LIMIT';
DATE_ESC: '{D';
TIME_ESC: '{T';
TIMESTAMP_ESC: '{TS';
// mapped to string literal
GUID_ESC: '{GUID';

ESC_END: '}';

// Operators
EQ  : '=';
NULLEQ: '<=>';
NEQ : '<>' | '!=';
LT  : '<';
LTE : '<=';
GT  : '>';
GTE : '>=';

PLUS: '+';
MINUS: '-';
ASTERISK: '*';
SLASH: '/';
PERCENT: '%';
CAST_OP: '::';
CONCAT: '||';
DOT: '.';
PARAM: '?';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    ;

INTEGER_VALUE
    : DIGIT+
    ;

DECIMAL_VALUE
    : DIGIT+ DOT DIGIT*
    | DOT DIGIT+
    | DIGIT+ (DOT DIGIT*)? EXPONENT
    | DOT DIGIT+ EXPONENT
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' )*
    ;

DIGIT_IDENTIFIER
    : DIGIT (LETTER | DIGIT | '_' | '@')+
    ;

TABLE_IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

QUOTED_IDENTIFIER
    : '"' ( ~'"' | '""' )* '"'
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
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
