/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.type.EsqlDataTypes;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.ReferenceAttribute;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Grok extends RegexExtract {

    public record Parser(String pattern, org.elasticsearch.grok.Grok grok) {

        private List<Attribute> extractedFields() {
            return grok.captureConfig()
                .stream()
                .sorted(Comparator.comparing(GrokCaptureConfig::name))
                // promote small numeric types, since Grok can produce float values
                .map(x -> new ReferenceAttribute(Source.EMPTY, x.name(), EsqlDataTypes.widenSmallNumericTypes(toDataType(x.type()))))
                .collect(Collectors.toList());
        }

        private static DataType toDataType(GrokCaptureType type) {
            return switch (type) {
                case STRING -> DataTypes.KEYWORD;
                case INTEGER -> DataTypes.INTEGER;
                case LONG -> DataTypes.LONG;
                case FLOAT -> DataTypes.FLOAT;
                case DOUBLE -> DataTypes.DOUBLE;
                case BOOLEAN -> DataTypes.BOOLEAN;
            };
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Parser parser = (Parser) o;
            return Objects.equals(pattern, parser.pattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern);
        }
    }

    public static Parser pattern(Source source, String pattern) {
        try {
            var builtinPatterns = GrokBuiltinPatterns.get(true);
            org.elasticsearch.grok.Grok grok = new org.elasticsearch.grok.Grok(builtinPatterns, pattern, logger::warn);
            return new Parser(pattern, grok);
        } catch (IllegalArgumentException e) {
            throw new ParsingException(source, "Invalid pattern [{}] for grok: {}", pattern, e.getMessage());
        }
    }

    private static final Logger logger = LogManager.getLogger(Grok.class);

    private final Parser parser;

    public Grok(Source source, LogicalPlan child, Expression inputExpression, Parser parser) {
        this(source, child, inputExpression, parser, parser.extractedFields());
    }

    public Grok(Source source, LogicalPlan child, Expression inputExpr, Parser parser, List<Attribute> extracted) {
        super(source, child, inputExpr, extracted);
        this.parser = parser;

    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Grok(source(), newChild, input, parser, extractedFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Grok::new, child(), input, parser, extractedFields);
    }

    @Override
    public List<Attribute> output() {
        return NamedExpressions.mergeOutputAttributes(extractedFields, child().output());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Grok grok = (Grok) o;
        return Objects.equals(parser, grok.parser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parser);
    }

    public Parser parser() {
        return parser;
    }
}
