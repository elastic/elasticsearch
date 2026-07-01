/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.grok.GrokBuiltinPatterns;
import org.elasticsearch.grok.GrokCaptureConfig;
import org.elasticsearch.grok.GrokCaptureType;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.NamedExpressions;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.parser.ParsingException;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Grok extends RegexExtract implements TelemetryAware, SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Grok", Grok::readFrom);

    public record Parser(String pattern, org.elasticsearch.grok.Grok grok) implements NodeStringRenderable {

        public List<Attribute> extractedFields() {
            return grok.captureConfig()
                .stream()
                .sorted(Comparator.comparing(GrokCaptureConfig::name))
                // promote small numeric types, since Grok can produce float values
                .map(x -> new ReferenceAttribute(Source.EMPTY, null, x.name(), toDataType(x.type()).widenSmallNumeric()))
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

        // Delegates to the mapper-aware render with IDENTITY so the two can never diverge.
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            nodeString(sb, NodeStringFormat.LIMITED, NodeStringMapper.IDENTITY);
            return sb.toString();
        }

        /**
         * Single render path for the parser. Under {@link NodeStringMapper#IDENTITY} this is
         * byte-identical to the legacy {@code toString()}; under an anonymizing mapper the capture
         * names route through the mapper while the Grok library identifiers, type suffixes, and
         * {@code %{...}} structure stay verbatim.
         */
        @Override
        public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
            sb.append("Parser[pattern=");
            rewriteGrokPattern(sb, pattern, mapper);
            sb.append("]");
        }
    }

    public static Parser pattern(Source source, String pattern) {
        return pattern(source, pattern, MatcherWatchdog.noop());
    }

    public static Parser pattern(Source source, String pattern, MatcherWatchdog matcherWatchdog) {
        try {
            var builtinPatterns = GrokBuiltinPatterns.get(true);
            org.elasticsearch.grok.Grok grok = new org.elasticsearch.grok.Grok(builtinPatterns, pattern, matcherWatchdog, logger::warn);
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

    private static Grok readFrom(StreamInput in) throws IOException {
        PlanStreamInput planIn = (PlanStreamInput) in;
        Source source = Source.readFrom(planIn);
        return new Grok(
            source,
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            Grok.pattern(source, in.readString(), planIn.grokMatcherWatchdog()),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(input());
        out.writeString(parser().pattern());
        out.writeNamedWriteableCollection(extractedFields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        return new Grok(source(), child(), input, parser, renameExtractedFields(newNames));
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

    /**
     * Renders a Grok pattern of shape {@code "%{IP:client_ip} %{NUMBER:bytes:int}"}. The Grok
     * library identifier (the part before the first {@code :}) is a predefined library name (IP,
     * NUMBER, DATA, ...), not customer data — passes through. The capture name routes through
     * {@code mapper.column}. The optional type-coercion suffix after the second colon passes
     * through.
     */
    public static void rewriteGrokPattern(StringBuilder sb, String pattern, NodeStringMapper mapper) {
        if (pattern == null || pattern.isEmpty()) {
            return;
        }
        int i = 0;
        while (i < pattern.length()) {
            int start = pattern.indexOf("%{", i);
            if (start < 0) {
                sb.append(pattern, i, pattern.length());
                return;
            }
            sb.append(pattern, i, start);
            int end = pattern.indexOf('}', start + 2);
            if (end < 0) {
                sb.append(pattern, start, pattern.length());
                return;
            }
            String body = pattern.substring(start + 2, end);
            int firstColon = body.indexOf(':');
            sb.append("%{");
            if (firstColon < 0) {
                sb.append(body);
            } else {
                String libraryId = body.substring(0, firstColon);
                String rest = body.substring(firstColon + 1);
                int secondColon = rest.indexOf(':');
                String captureName = secondColon < 0 ? rest : rest.substring(0, secondColon);
                String suffix = secondColon < 0 ? "" : rest.substring(secondColon);
                sb.append(libraryId).append(':').append(mapper.column(captureName)).append(suffix);
            }
            sb.append('}');
            i = end + 1;
        }
    }
}
