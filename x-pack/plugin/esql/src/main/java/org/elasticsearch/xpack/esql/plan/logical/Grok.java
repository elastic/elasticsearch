/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureType;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Grok extends RegexExtract {

    public record Parser(String pattern, org.elasticsearch.grok.Grok grok) {

        public List<Attribute> extractedFields() {
            return grok.captureConfig()
                .stream()
                .sorted(Comparator.comparing(GrokCaptureConfig::name))
                // promote small numeric types, since Grok can produce float values
                .map(x -> new ReferenceAttribute(Source.EMPTY, x.name(), toDataType(x.type()).widenSmallNumeric()))
                .collect(Collectors.toList());
        }

        private static DataType toDataType(GrokCaptureType type) {
            return switch (type) {
                case STRING -> DataType.KEYWORD;
                case INTEGER -> DataType.INTEGER;
                case LONG -> DataType.LONG;
                case FLOAT -> DataType.FLOAT;
                case DOUBLE -> DataType.DOUBLE;
                case BOOLEAN -> DataType.BOOLEAN;
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
    public Grok withGeneratedNames(List<String> newNames) {
        if (newNames.size() != extractedFields.size()) {
            throw new IllegalArgumentException(
                "Number of new names is [" + newNames.size() + "] but there are [" + extractedFields.size() + "] existing names."
            );
        }

        List<Attribute> renamedExtractedFields = new ArrayList<>(extractedFields.size());
        for (int i = 0; i < newNames.size(); i++) {
            Attribute extractedField = extractedFields.get(i);
            String newName = newNames.get(i);
            if (extractedField.name().equals(newName)) {
                renamedExtractedFields.add(extractedField);
            } else {
                renamedExtractedFields.add(extractedFields.get(i).withName(newNames.get(i)).withId(new NameId()));
            }
        }

        return new Grok(source(), child(), input, parser, renamedExtractedFields);
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
