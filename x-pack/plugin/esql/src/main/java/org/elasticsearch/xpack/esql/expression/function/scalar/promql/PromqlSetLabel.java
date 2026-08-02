/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.promql;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;

/**
 * Internal, non-user-callable scalar that writes a derived label into the {@code _timeseries} identity blob for the PromQL
 * label functions. It consumes the identity blob and the value produced by {@link PromqlRegexExtract} (or, for
 * {@code label_join}, by {@code CONCAT}) and applies the three Prometheus outcomes:
 * <ul>
 *     <li><b>no-op</b> - a {@code null} value leaves the blob untouched (used by {@code label_replace}'s no-match);</li>
 *     <li><b>delete</b> - an empty value removes {@code dstName} from the blob;</li>
 *     <li><b>set</b> - a non-empty value writes {@code dstName}.</li>
 * </ul>
 * The blob carries exactly one passthrough namespace ({@code labels} for Prometheus data, {@code attributes} for OTel). Every
 * label writes into that namespace except {@code __name__}, which is written as {@code labels.__name__} for Prometheus data
 * and as a bare top-level key for OTel data (so it surfaces bare) - matching how the identity blob is rendered.
 * <p>
 * Output keys are emitted in a canonical (lexicographically sorted) order so that two series whose label sets become equal
 * after relabeling produce byte-identical blobs, and therefore collapse/merge on the same grouping key.
 */
public final class PromqlSetLabel extends EsqlScalarFunction implements VersionedNamedWriteable {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "PromqlSetLabel",
        PromqlSetLabel::new
    );

    static final String NAME_LABEL = "__name__";
    static final String PROMETHEUS_NAMESPACE = "labels";
    static final String OTEL_NAMESPACE = "attributes";

    private final Expression timeseries;
    private final Expression value;
    private final Expression dstName;

    public PromqlSetLabel(Source source, Expression timeseries, Expression value, Expression dstName) {
        super(source, List.of(timeseries, value, dstName));
        this.timeseries = timeseries;
        this.value = value;
        this.dstName = dstName;
    }

    private PromqlSetLabel(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(timeseries);
        out.writeNamedWriteable(value);
        out.writeNamedWriteable(dstName);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return PromqlRegexExtract.PROMQL_LABEL_FUNCTIONS;
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }
        return isString(timeseries, sourceText(), FIRST).and(isString(value, sourceText(), SECOND))
            .and(isString(dstName, sourceText(), THIRD));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new PromqlSetLabel(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, PromqlSetLabel::new, timeseries, value, dstName);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (dstName.foldable() == false) {
            throw new IllegalArgumentException("[dstName] must be a constant");
        }
        BytesRef dstNameValue = BytesRefs.toBytesRef(dstName.fold(toEvaluator.foldCtx()));
        return new PromqlSetLabelEvaluator.Factory(source(), toEvaluator.apply(timeseries), toEvaluator.apply(value), dstNameValue);
    }

    /**
     * Applies the label write/delete to the identity blob and returns it in canonical (sorted-key) JSON form. The value
     * encodes the three Prometheus outcomes: the no-op case ({@code null} value) is handled by the caller (the evaluator
     * leaves the blob untouched); an empty value ({@code delete == true}) removes {@code name}; a non-empty value sets it.
     * <p>
     * The output must always be sorted, because it becomes a grouping/collapse key: two series whose label sets become equal
     * after relabeling have to serialize to byte-identical blobs to merge on the same key. Sorting is therefore
     * unconditional - it cannot rely on the input being sorted, since even a sorted input becomes unsorted once a label is
     * added at a new position (e.g. {@code label_replace} inserting a key that sorts before an existing one).
     * <p>
     * The blob is parsed once into sorted maps whose values are captured as raw JSON fragments (see {@link #capture}), so
     * untouched labels are never decoded to strings or boxed and their JSON type is preserved (an OTel attribute may be
     * numeric or boolean, not just a string). Only the written label's name and value are materialized explicitly. The tree is
     * then re-serialized in lexicographic key order at every object level, matching the ordering the synthetic
     * {@code _timeseries} source emits.
     */
    static BytesRef rewrite(BytesRef blob, String name, BytesRef labelValue, boolean delete) throws IOException {
        TreeMap<String, Node> root = parse(blob);
        applyLabel(root, name, labelValue, delete);
        try (XContentBuilder out = JsonXContent.contentBuilder()) {
            writeObject(out, root);
            return BytesReference.bytes(out).toBytesRef();
        }
    }

    /**
     * A value in the parsed identity tree. Modelled as a sealed hierarchy so the emit switch stays exhaustive and no value is
     * ever handled by an unchecked cast.
     */
    private sealed interface Node {}

    /**
     * A scalar or array value carried through as a raw JSON fragment, without decoding it to a Java value (no string
     * conversion, no boxing). Re-serialized as-is by the same generator, so its JSON type is preserved.
     */
    private record Verbatim(BytesReference bytes) implements Node {}

    /** A JSON object whose entries are kept in sorted key order; also used for the passthrough namespaces and the root. */
    private record Obj(TreeMap<String, Node> entries) implements Node {}

    /** A label value to write; serialized as a JSON string from its raw UTF-8 bytes only at emit time. */
    private record NewLabel(BytesRef value) implements Node {}

    /**
     * Parses the blob into a sorted tree. Every object level becomes a key-sorted {@link Obj}; scalars and arrays are captured
     * verbatim. A single pass reads each field name (unavoidably as a {@code String}) and copies its value bytes untouched.
     */
    private static TreeMap<String, Node> parse(BytesRef blob) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                XContentParserConfiguration.EMPTY,
                blob.bytes,
                blob.offset,
                blob.length
            )
        ) {
            parser.nextToken(); // START_OBJECT (root)
            return parseObject(parser);
        }
    }

    /** Reads an object (parser positioned on its {@code START_OBJECT}) into a sorted map of captured values. */
    private static TreeMap<String, Node> parseObject(XContentParser parser) throws IOException {
        TreeMap<String, Node> entries = new TreeMap<>();
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            String fieldName = parser.currentName();
            parser.nextToken(); // value token
            entries.put(fieldName, capture(parser));
        }
        return entries;
    }

    /**
     * Captures the value the parser is positioned on. Objects recurse (so they too come out sorted); scalars and arrays are
     * copied into a raw JSON fragment without decoding to a Java value, preserving their JSON type with no string/boxing round
     * trip. Array elements are not reordered, matching the previous canonicalization which sorted map keys only.
     */
    private static Node capture(XContentParser parser) throws IOException {
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return new Obj(parseObject(parser));
        }
        try (XContentBuilder fragment = JsonXContent.contentBuilder()) {
            fragment.copyCurrentStructure(parser);
            return new Verbatim(BytesReference.bytes(fragment));
        }
    }

    /**
     * Writes the label into the tree. Prometheus data keeps {@code __name__} inside {@code labels}; OTel data (or any blob
     * without a {@code labels} namespace) surfaces it as a bare top-level key. Every other label writes into the single
     * passthrough namespace the blob carries ({@code attributes} only when it is present and {@code labels} is not).
     */
    private static void applyLabel(TreeMap<String, Node> root, String name, BytesRef labelValue, boolean delete) {
        if (NAME_LABEL.equals(name)) {
            Obj labels = objectAt(root, PROMETHEUS_NAMESPACE);
            if (labels != null) {
                put(labels.entries(), NAME_LABEL, labelValue, delete);
            } else if (delete) {
                root.remove(NAME_LABEL);
            } else {
                root.put(NAME_LABEL, new NewLabel(labelValue));
            }
            return;
        }
        String ns = objectAt(root, OTEL_NAMESPACE) != null && objectAt(root, PROMETHEUS_NAMESPACE) == null
            ? OTEL_NAMESPACE
            : PROMETHEUS_NAMESPACE;
        Obj namespace = objectAt(root, ns);
        if (namespace == null) {
            if (delete) {
                return; // nothing to remove from an absent namespace, and no reason to materialize an empty one
            }
            namespace = new Obj(new TreeMap<>());
            root.put(ns, namespace);
        }
        put(namespace.entries(), name, labelValue, delete);
    }

    private static void put(TreeMap<String, Node> entries, String key, BytesRef labelValue, boolean delete) {
        if (delete) {
            entries.remove(key);
        } else {
            entries.put(key, new NewLabel(labelValue));
        }
    }

    private static Obj objectAt(TreeMap<String, Node> root, String key) {
        return root.get(key) instanceof Obj obj ? obj : null;
    }

    private static void writeObject(XContentBuilder out, TreeMap<String, Node> entries) throws IOException {
        out.startObject();
        for (Map.Entry<String, Node> entry : entries.entrySet()) {
            String name = entry.getKey();
            switch (entry.getValue()) {
                case Obj obj -> {
                    out.field(name);
                    writeObject(out, obj.entries());
                }
                case NewLabel label -> {
                    out.field(name);
                    out.utf8Value(label.value().bytes, label.value().offset, label.value().length);
                }
                // Untouched value: copy its raw bytes straight through (a raw byte copy for same-type JSON), no re-encoding.
                case Verbatim verbatim -> out.rawField(name, verbatim.bytes().streamInput(), XContentType.JSON);
            }
        }
        out.endObject();
    }
}
