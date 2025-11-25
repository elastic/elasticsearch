/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
parser grammar PromqlBaseParser;

@header {
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
}

options {
  superClass=ParserConfig;
  tokenVocab=PromqlBaseLexer;
}

singleStatement
    : expression EOF
    ;

// operator precedence defined in Promql at
// https://prometheus.io/docs/prometheus/latest/querying/operators/#binary-operator-precedence

expression
    : <assoc=right> left=expression op=CARET modifier? right=expression                         #arithmeticBinary
    | operator=(PLUS | MINUS) expression                                                        #arithmeticUnary
    | left=expression op=(ASTERISK | PERCENT | SLASH) modifier? right=expression                #arithmeticBinary
    | left=expression op=(MINUS | PLUS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=(EQ | NEQ | GT | GTE | LT | LTE) BOOL? modifier? right=expression      #arithmeticBinary
    | left=expression op=(AND | UNLESS) modifier? right=expression                              #arithmeticBinary
    | left=expression op=OR modifier? right=expression                                          #arithmeticBinary
    | value                                                                                     #valueExpression
    | LP expression RP                                                                          #parenthesized
    | expression LSB range=duration subqueryResolution RSB evaluation?                          #subquery
    ;

subqueryResolution
    : COLON (resolution=duration)?
    | <assoc=right> TIME_VALUE_WITH_COLON op=CARET expression
    | TIME_VALUE_WITH_COLON op=(ASTERISK | SLASH) expression
    | TIME_VALUE_WITH_COLON op=(MINUS|PLUS) expression
    | TIME_VALUE_WITH_COLON
    ;

value
    : function
    | selector
    | constant
    ;

function
    : IDENTIFIER LP functionParams? RP
    | IDENTIFIER LP functionParams RP grouping
    | IDENTIFIER grouping LP functionParams RP
    ;

functionParams
    : expression (COMMA expression)*
    ;

grouping
    : (BY | WITHOUT) labelList
    ;

selector
    : seriesMatcher (LSB duration RSB)? evaluation?
    ;

seriesMatcher
    : identifier (LCB labels? RCB)?
    | LCB labels RCB
    ;

modifier
    : matching=(IGNORING | ON) modifierLabels=labelList (joining=(GROUP_LEFT | GROUP_RIGHT) groupLabels=labelList?)?
    ;

// NB: PromQL explicitly allows a trailing comma for label enumeration
// both inside aggregation functions and metric labels.
labelList
    : LP (labelName COMMA?)* RP
    ;

labels
    :  label (COMMA label?)*
    ;

label
    : labelName (kind=(LABEL_EQ | NEQ | LABEL_RGX | LABEL_RGX_NEQ) STRING)?
    ;

labelName
    : identifier
    | STRING
    | number
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

evaluation
    : offset at?
    | at offset?
    ;

offset
    : OFFSET MINUS? duration
    ;

// do timeunit validation and break-down inside the parser
// this helps deal with ambiguities for multi-unit declarations (1d3m)
// and give better error messages
// support arithmetic for duration with partial support added in Prometheus 3.4
// https://github.com/prometheus/prometheus/issues/12318
// https://github.com/prometheus/prometheus/pull/16249
duration
    //: time_value
    : expression
    ;
at
    : AT MINUS? timeValue
    | AT (AT_START | AT_END)
    ;

constant
    : number
    | string
    | timeValue
    ;

number
    : DECIMAL_VALUE  #decimalLiteral
    | INTEGER_VALUE  #integerLiteral
    | HEXADECIMAL    #hexLiteral
    ;

string
    : STRING
    ;

timeValue
    : TIME_VALUE_WITH_COLON
    | TIME_VALUE
    | number
    ;

// declared tokens that can be used without special escaping
// in PromQL this applies to all keywords
nonReserved
    : AND
    | BOOL
    | BY
    | GROUP_LEFT
    | GROUP_RIGHT
    | IGNORING
    | OFFSET
    | OR
    | ON
    | UNLESS
    | WITHOUT
    ;
