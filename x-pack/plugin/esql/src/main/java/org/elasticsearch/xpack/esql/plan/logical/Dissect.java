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
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Dissect extends RegexExtract {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(LogicalPlan.class, "Dissect", Dissect::new);

    private final Parser parser;

    public record Parser(String pattern, String appendSeparator, DissectParser parser) implements Writeable {
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
                    keys.add(new ReferenceAttribute(src, x, DataType.KEYWORD));
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
}
