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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.dissect.DissectParser;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeStringMapper;
import org.elasticsearch.xpack.esql.core.tree.NodeStringRenderable;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Dissect extends RegexExtract implements TelemetryAware, SortPreserving {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Dissect", Dissect::new);

    private final Parser parser;

    public record Parser(String pattern, String appendSeparator, DissectParser parser) implements Writeable, NodeStringRenderable {
        public static Parser readFrom(StreamInput in) throws IOException {
            String pattern = in.readString();
            String appendSeparator = in.readString();
            return new Parser(pattern, appendSeparator, new DissectParser(pattern, appendSeparator));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(pattern());
            out.writeString(appendSeparator());
        }

        public List<Attribute> keyAttributes(Source src) {
            List<Attribute> keys = new ArrayList<>();
            for (var x : parser.outputKeys()) {
                if (x.isEmpty() == false) {
                    keys.add(new ReferenceAttribute(src, null, x, DataType.KEYWORD));
                }
            }

            return keys;
        }

        // Override hashCode and equals since the parser is considered equal if its pattern and
        // appendSeparator are equal ( and DissectParser uses reference equality )
        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (other == null || getClass() != other.getClass()) return false;
            Parser that = (Parser) other;
            return Objects.equals(this.pattern, that.pattern) && Objects.equals(this.appendSeparator, that.appendSeparator);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pattern, appendSeparator);
        }

        // Needed for consistent output since parser's toString isn't overriden. Delegates to the
        // mapper-aware render with IDENTITY so the two can never diverge.
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            nodeString(sb, NodeStringFormat.LIMITED, NodeStringMapper.IDENTITY);
            return sb.toString();
        }

        /**
         * Single render path for the parser. Under {@link NodeStringMapper#IDENTITY} this is
         * byte-identical to the legacy {@code toString()} (the appendSeparator is non-null at every
         * construction site); under an anonymizing mapper the pattern capture names and the append
         * separator route through the mapper, the {@code %{...}} braces and the {@code DissectParser}
         * class name stay verbatim.
         */
        @Override
        public void nodeString(StringBuilder sb, NodeStringFormat format, NodeStringMapper mapper) {
            sb.append("Parser[pattern=");
            rewriteDissectPattern(sb, pattern, mapper);
            sb.append(", appendSeparator=");
            if (appendSeparator != null && appendSeparator.isEmpty() == false) {
                sb.append(mapper.column(appendSeparator));
            }
            sb.append(", parser=").append(parser.getClass().getSimpleName()).append("]");
        }

    }

    public Dissect(Source source, LogicalPlan child, Expression input, Parser parser, List<Attribute> extracted) {
        super(source, child, input, extracted);
        this.parser = parser;
    }

    private Dissect(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            Parser.readFrom(in),
            in.readNamedWriteableCollectionAsList(Attribute.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeNamedWriteable(child());
        out.writeNamedWriteable(input());
        parser.writeTo(out);
        out.writeNamedWriteableCollection(extractedFields());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public UnaryPlan replaceChild(LogicalPlan newChild) {
        return new Dissect(source(), newChild, input, parser, extractedFields);
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(this, Dissect::new, child(), input, parser, extractedFields);
    }

    @Override
    public Dissect withGeneratedNames(List<String> newNames) {
        return new Dissect(source(), child(), input, parser, renameExtractedFields(newNames));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        Dissect dissect = (Dissect) o;
        return Objects.equals(parser, dissect.parser);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), parser);
    }

    public Parser parser() {
        return parser;
    }

    /**
     * Renders a Dissect pattern of shape {@code "%{cap1} sep %{cap2}"} routing each capture
     * identifier through {@code mapper.column}. Captures may carry a {@code ?} (skip) or
     * {@code +} (append) modifier right after the open brace — that's structural, preserved
     * verbatim. Separator characters between captures pass through.
     */
    public static void rewriteDissectPattern(StringBuilder sb, String pattern, NodeStringMapper mapper) {
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
            String modifier = "";
            String captureName = body;
            if (body.startsWith("?") || body.startsWith("+")) {
                modifier = body.substring(0, 1);
                captureName = body.substring(1);
            }
            sb.append("%{").append(modifier).append(captureName.isEmpty() ? "" : mapper.column(captureName)).append('}');
            i = end + 1;
        }
    }
}
