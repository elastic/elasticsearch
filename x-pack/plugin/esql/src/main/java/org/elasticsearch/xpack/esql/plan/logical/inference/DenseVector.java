/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.xpack.esql.capabilities.PostAnalysisVerificationAware;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.common.Failures;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.common.Failure.fail;
import static org.elasticsearch.xpack.esql.expression.NamedExpressions.mergeOutputAttributes;

/**
 * The {@code DENSE_VECTOR} command generates a {@code dense_vector} embedding column per input field.
 * <p>
 * It takes an explicit comma-separated list of field names and, for each text field, appends a generated
 * {@code <field>_dense_vector} column. Unlike {@code KEEP}, it adds columns rather than projecting them.
 * </p>
 */
public class DenseVector extends InferencePlan<DenseVector> implements TelemetryAware, PostAnalysisVerificationAware {

    /** Suffix appended to each input field name to build the generated column name (e.g. {@code title} -> {@code title_dense_vector}). */
    public static final String OUTPUT_SUFFIX = "_dense_vector";

    public static final String TIMEOUT_OPTION_NAME = "timeout";

    /**
     * Name of the {@code WITH} option selecting the input modality ({@code text} or {@code image}). Defaults to {@code text}.
     * The value maps to an {@link org.elasticsearch.inference.DataType}; the corresponding {@link org.elasticsearch.inference.DataFormat}
     * is derived from that type (see {@link org.elasticsearch.inference.DataType#getDefaultFormat()}).
     */
    public static final String TYPE_OPTION_NAME = "type";

    /** Default input modality when the {@code type} option is not provided. */
    public static final org.elasticsearch.inference.DataType DEFAULT_INPUT_TYPE = org.elasticsearch.inference.DataType.TEXT;

    /**
     * Built-in default inference endpoint used when the query provides no {@code inference_id} (via {@code WITH}) and no
     * cluster-level default ({@code esql.command.dense_vector.default_inference_id}) is configured. This is the E5 text
     * embedding endpoint that is registered on ML-capable nodes, so it works with zero configuration.
     */
    public static final String DEFAULT_INFERENCE_ID = ".multilingual-e5-small-elasticsearch";

    /**
     * Preconfigured endpoint served by the Elastic Inference Service, available on deployments that reach it — including
     * serverless, which runs no ML nodes. Mirrors the endpoint {@code semantic_text} prefers for dense text
     * ({@code SemanticTextFieldMapper.DEFAULT_EIS_JINA_V5_INFERENCE_ID}); the literal is repeated here because the inference
     * plugin is not on this module's compile classpath.
     */
    public static final String EIS_JINA_V5_INFERENCE_ID = ".jina-embeddings-v5-text-small";

    /**
     * Endpoints tried in order when {@link #inferenceIdIsFallback()} holds, to pick one that exists on this deployment. Both are
     * dense text embedding endpoints, so either can serve a {@code text} input.
     */
    public static final List<String> DEFAULT_INFERENCE_ID_CANDIDATES = List.of(EIS_JINA_V5_INFERENCE_ID, DEFAULT_INFERENCE_ID);

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        LogicalPlan.class,
        "DenseVector",
        DenseVector::new
    );

    /** Input fields to embed (unresolved names before analysis, resolved attributes after). */
    private final List<NamedExpression> fields;

    /** Input modality ({@code text} or {@code image}) selected by the {@code type} option. */
    private final org.elasticsearch.inference.DataType inputType;

    /**
     * Task type of the resolved inference endpoint. {@code null} until the analyzer resolves the endpoint; the planner uses it
     * to route text inputs to the request shape the endpoint accepts (a {@link TaskType#TEXT_EMBEDDING} endpoint takes a text
     * embedding request, a {@link TaskType#EMBEDDING} endpoint takes an embedding request).
     */
    private final TaskType endpointTaskType;

    /**
     * The generated {@code <field>_dense_vector} attributes, one per input field, appended to the child's output in the
     * same order as {@link #fields}. Empty until the analyzer resolves/expands {@link #fields} and populates them.
     */
    private final List<Attribute> generatedFields;

    /**
     * Whether {@link #inferenceId()} holds {@link #DEFAULT_INFERENCE_ID} because neither the query nor the cluster setting named
     * an endpoint, as opposed to a query naming that same endpoint explicitly. The id alone cannot tell the two apart, since
     * {@link Literal#equals} ignores source.
     */
    private final boolean inferenceIdIsFallback;

    private List<Attribute> lazyOutput;

    public DenseVector(Source source, LogicalPlan child, Expression rowLimit, List<NamedExpression> fields) {
        this(
            source,
            child,
            Literal.keyword(Source.EMPTY, DEFAULT_INFERENCE_ID),
            rowLimit,
            fields,
            List.of(),
            null,
            DEFAULT_INPUT_TYPE,
            null,
            true
        );
    }

    public DenseVector(
        Source source,
        LogicalPlan child,
        Expression inferenceId,
        Expression rowLimit,
        List<NamedExpression> fields,
        List<Attribute> generatedFields,
        TimeValue timeout,
        org.elasticsearch.inference.DataType inputType,
        TaskType endpointTaskType,
        boolean inferenceIdIsFallback
    ) {
        super(source, child, inferenceId, rowLimit, timeout);
        this.fields = fields;
        this.generatedFields = generatedFields;
        this.inputType = inputType;
        this.endpointTaskType = endpointTaskType;
        this.inferenceIdIsFallback = inferenceIdIsFallback;
    }

    public DenseVector(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(LogicalPlan.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteableCollectionAsList(NamedExpression.class),
            in.readNamedWriteableCollectionAsList(Attribute.class),
            in.getTransportVersion().supports(ESQL_INFERENCE_ACCEPT_TIMEOUT) ? in.readOptionalTimeValue() : null,
            in.getTransportVersion().supports(ESQL_DENSE_VECTOR_TYPE_OPTION)
                ? org.elasticsearch.inference.DataType.fromString(in.readString())
                : DEFAULT_INPUT_TYPE,
            // Nodes without the type option have no multimodal support, so their plans only ever describe text embedding
            // requests.
            in.getTransportVersion().supports(ESQL_DENSE_VECTOR_TYPE_OPTION)
                ? in.readOptionalEnum(TaskType.class)
                : TaskType.TEXT_EMBEDDING,
            in.getTransportVersion().supports(ESQL_DENSE_VECTOR_FALLBACK_INFERENCE_ID) && in.readBoolean()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteableCollection(fields);
        out.writeNamedWriteableCollection(generatedFields);
        if (out.getTransportVersion().supports(ESQL_INFERENCE_ACCEPT_TIMEOUT)) {
            out.writeOptionalTimeValue(timeout());
        }
        if (out.getTransportVersion().supports(ESQL_DENSE_VECTOR_TYPE_OPTION)) {
            out.writeString(inputType.name());
            out.writeOptionalEnum(endpointTaskType);
        }
        if (out.getTransportVersion().supports(ESQL_DENSE_VECTOR_FALLBACK_INFERENCE_ID)) {
            out.writeBoolean(inferenceIdIsFallback);
        }
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public List<NamedExpression> fields() {
        return fields;
    }

    /**
     * Builds nullable {@code <field>_dense_vector} attributes (one per input field), typed {@link DataType#DENSE_VECTOR}.
     * A generated name that collides with an existing output shadows the earlier column.
     */
    public static List<Attribute> generatedAttributesFor(Source source, List<? extends NamedExpression> fields) {
        return fields.stream()
            .map(
                f -> (Attribute) new ReferenceAttribute(
                    f.source(),
                    null,
                    f.name() + OUTPUT_SUFFIX,
                    DataType.DENSE_VECTOR,
                    Nullability.TRUE,
                    null,
                    false
                )
            )
            .toList();
    }

    /**
     * Returns a copy using {@code newInferenceId}, which is by definition not a fallback: the caller named this endpoint. The
     * fallback state is part of the identity check because a query may name {@link #DEFAULT_INFERENCE_ID} itself, in which case
     * the id is unchanged but the node is not.
     */
    @Override
    public DenseVector withInferenceId(Expression newInferenceId) {
        if (inferenceIdIsFallback == false && inferenceId().equals(newInferenceId)) {
            return this;
        }
        return new DenseVector(
            source(),
            child(),
            newInferenceId,
            rowLimit(),
            fields,
            generatedFields,
            timeout(),
            inputType,
            endpointTaskType,
            false
        );
    }

    @Override
    public DenseVector withTimeout(TimeValue newTimeout) {
        if (Objects.equals(timeout(), newTimeout)) {
            return this;
        }
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            fields,
            generatedFields,
            newTimeout,
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    /** Input modality selected by the {@code type} option. */
    public org.elasticsearch.inference.DataType inputType() {
        return inputType;
    }

    public DenseVector withInputType(org.elasticsearch.inference.DataType newInputType) {
        if (inputType == newInputType) {
            return this;
        }
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            fields,
            generatedFields,
            timeout(),
            newInputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    /**
     * Whether {@link #inferenceId()} is the built-in {@link #DEFAULT_INFERENCE_ID} fallback rather than an endpoint named by the
     * query or the cluster setting.
     */
    public boolean inferenceIdIsFallback() {
        return inferenceIdIsFallback;
    }

    /** Task type of the resolved inference endpoint, or {@code null} before analysis resolves it. */
    public TaskType endpointTaskType() {
        return endpointTaskType;
    }

    public DenseVector withEndpointTaskType(TaskType newEndpointTaskType) {
        if (endpointTaskType == newEndpointTaskType) {
            return this;
        }
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            fields,
            generatedFields,
            timeout(),
            inputType,
            newEndpointTaskType,
            inferenceIdIsFallback
        );
    }

    @Override
    public DenseVector replaceChild(LogicalPlan newChild) {
        return new DenseVector(
            source(),
            newChild,
            inferenceId(),
            rowLimit(),
            fields,
            generatedFields,
            timeout(),
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    /**
     * Returns a copy with resolved input fields and the matching generated {@code <field>_dense_vector}
     * attributes. Used by the analyzer once {@link #fields} are resolved against the child output.
     */
    public DenseVector withResolvedFields(List<NamedExpression> resolvedFields, List<Attribute> resolvedGeneratedFields) {
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            resolvedFields,
            resolvedGeneratedFields,
            timeout(),
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    /**
     * Returns a copy retaining only the given input fields and their matching generated attributes. The two lists must be
     * aligned 1:1 (each {@code field} with its {@code <field>_dense_vector} attribute). Used by column pruning to drop the
     * fields whose generated column is unused, avoiding wasted inference calls.
     */
    public DenseVector withPrunedFields(List<NamedExpression> prunedFields, List<Attribute> prunedGeneratedFields) {
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            prunedFields,
            prunedGeneratedFields,
            timeout(),
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    @Override
    public List<String> validOptionNames() {
        return List.of(INFERENCE_ID_OPTION_NAME, TIMEOUT_OPTION_NAME, TYPE_OPTION_NAME);
    }

    @Override
    public TaskType taskType() {
        return TaskType.TEXT_EMBEDDING;
    }

    /**
     * Lists every candidate in {@link #DEFAULT_INFERENCE_ID_CANDIDATES} while the endpoint is an unresolved fallback for a
     * {@code text} input, so analysis can select whichever candidate this deployment has.
     */
    @Override
    public List<String> candidateInferenceIds() {
        return inferenceIdIsFallback && inputType == org.elasticsearch.inference.DataType.TEXT
            ? DEFAULT_INFERENCE_ID_CANDIDATES
            : super.candidateInferenceIds();
    }

    /**
     * Accepted endpoint task types by input modality. {@code text} runs against a {@link TaskType#TEXT_EMBEDDING} or
     * {@link TaskType#EMBEDDING} endpoint; {@code image} requires a multimodal {@link TaskType#EMBEDDING} endpoint.
     */
    @Override
    public EnumSet<TaskType> acceptedTaskTypes() {
        return inputType == org.elasticsearch.inference.DataType.TEXT
            ? EnumSet.of(TaskType.TEXT_EMBEDDING, TaskType.EMBEDDING)
            : EnumSet.of(TaskType.EMBEDDING);
    }

    @Override
    public List<Attribute> output() {
        if (lazyOutput == null) {
            lazyOutput = mergeOutputAttributes(generatedFields, child().output());
        }
        return lazyOutput;
    }

    @Override
    public List<Attribute> generatedAttributes() {
        return generatedFields;
    }

    @Override
    public DenseVector withGeneratedNames(List<String> newNames) {
        checkNumberOfNewNames(newNames);
        List<Attribute> renamed = new ArrayList<>(generatedFields.size());
        for (int i = 0; i < generatedFields.size(); i++) {
            Attribute attr = generatedFields.get(i);
            String newName = newNames.get(i);
            renamed.add(newName.equals(attr.name()) ? attr : attr.withName(newName).withId(new NameId()));
        }
        return new DenseVector(
            source(),
            child(),
            inferenceId(),
            rowLimit(),
            fields,
            renamed,
            timeout(),
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    @Override
    protected AttributeSet computeReferences() {
        // Only the input fields are references; the generated <field>_dense_vector columns are outputs.
        return Expressions.references(fields);
    }

    @Override
    public boolean expressionsResolved() {
        if (super.expressionsResolved() == false) {
            return false;
        }
        for (NamedExpression field : fields) {
            if (field.resolved() == false) {
                return false;
            }
        }
        // An empty field list is a resolved no-op: the command generates no columns.
        // Non-empty results always have generatedFields populated 1:1 with fields.
        return true;
    }

    @Override
    public boolean isFoldable() {
        return fields.stream().allMatch(Expression::foldable);
    }

    @Override
    public void postAnalysisVerification(Failures failures) {
        for (NamedExpression field : fields) {
            if (field.resolved() && DataType.isString(field.dataType()) == false) {
                failures.add(
                    fail(
                        field,
                        "DENSE_VECTOR field [{}] must be [text] or [keyword], found [{}]",
                        field.name(),
                        field.dataType().typeName()
                    )
                );
            }
        }
    }

    @Override
    protected NodeInfo<? extends LogicalPlan> info() {
        return NodeInfo.create(
            this,
            DenseVector::new,
            child(),
            inferenceId(),
            rowLimit(),
            fields,
            generatedFields,
            timeout(),
            inputType,
            endpointTaskType,
            inferenceIdIsFallback
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        DenseVector other = (DenseVector) o;
        return Objects.equals(fields, other.fields)
            && Objects.equals(generatedFields, other.generatedFields)
            && inputType == other.inputType
            && endpointTaskType == other.endpointTaskType
            && inferenceIdIsFallback == other.inferenceIdIsFallback;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), fields, generatedFields, inputType, endpointTaskType, inferenceIdIsFallback);
    }

    @Override
    public String nodeName() {
        return "DENSE_VECTOR";
    }
}
