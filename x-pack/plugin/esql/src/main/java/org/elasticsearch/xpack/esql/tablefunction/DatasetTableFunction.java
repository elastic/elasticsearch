/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceAuthorizer;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetService;
import org.elasticsearch.xpack.esql.datasources.dataset.InlineDatasetDefinition;
import org.elasticsearch.xpack.esql.datasources.dataset.PutDatasetAction;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code DATASET(<data source>, {<dataset definition>})} — reads a dataset that was never registered, against a data
 * source that was.
 * <p>
 * The two arguments are the two halves a registered dataset is made of. The data source is named, and supplies the
 * credentials and the endpoint, which a query cannot state and a node binds from its own cluster state. The payload
 * is the rest of that dataset's definition — the same JSON the dataset API carries, checked by the same validator
 * ({@link DatasetService#validateDefinition}), so the two ways of defining a dataset cannot drift and so a credential
 * cannot be written into a query text, that validator already refusing a setting the data source holds as a secret.
 * <p>
 * The definition is not registered anywhere and the leaf it builds carries no dataset name. What authorizes the read
 * is therefore the data source alone: {@code read} under {@code global.data_source}, the same privilege spent when a
 * dataset is attached to a data source, which is the point — a privilege meaning "query out of this data source"
 * should not turn on whether somebody registered a dataset first. The coordinator spends it through
 * {@code EsqlAuthorizeDatasourceAction} before any of this runs.
 */
public final class DatasetTableFunction implements TableFunction, BindsOnDataNode {

    public static final String NAME = "dataset";

    /**
     * The name the built {@link Dataset} carries, since an inline definition claims none. Deliberately not a legal
     * dataset name: it reaches no cluster state and no cache address ({@code DefinitionVersion.of} folds the resource
     * and the settings, never the name), but it DOES reach diagnostics and operator labels through
     * {@code ExternalSourceResolver#DATASET_CONTEXT_KEY}. A real data source's name would read there as a dataset,
     * and a dataset may legitimately share a data source's name — the two live in different namespaces — so a failure
     * would be attributed to a dataset that exists and was never read.
     */
    static final String INLINE_DATASET_NAME = "<inline>";

    @Override
    public String name() {
        return NAME;
    }

    /**
     * The registration API's per-type validators, so an inline definition meets the bar a registered one does.
     * Held here rather than handed in with the project, because they are this function's dependency and no other
     * function's — the facility carries only what every function needs.
     */
    private final Map<String, DataSourceValidator> dataSourceValidators;

    public DatasetTableFunction(Map<String, DataSourceValidator> dataSourceValidators) {
        this.dataSourceValidators = Map.copyOf(dataSourceValidators);
    }

    /**
     * The data source and the resource as plain arguments, then the rest of the definition as one trailing map —
     * which is how a nested chunk of structure is passed everywhere else in the language. {@code COMPLETION} is the
     * closest example: {@code WITH { "inference_id": "...", "task_settings": {"temperature": 1} }} puts its nested
     * object inside the one map under a named key, rather than taking a second map for it.
     * <p>
     * The map is the dataset definition's JSON, at full fidelity: every key a registered definition may carry is
     * accepted here, holding exactly the object the registration API takes, and nothing inside any of them is
     * rearranged. The one difference is the resource, lifted out to its own argument because every call needs one
     * and a required thing reads better as an argument than as a key — and refused inside the map, where it would
     * then have two homes.
     */
    private static final TableFunctionSignature SIGNATURE = TableFunctionSignature.of(
        TableFunctionSignature.name("data source"),
        TableFunctionSignature.name("resource"),
        TableFunctionSignature.optional(TableFunctionSignature.namedValues("options"))
    );

    @Override
    public TableFunctionSignature signature() {
        return SIGNATURE;
    }

    @Override
    public void validate(TableFunctionCall call) {
        // One data source, never a pattern: the credentials a read runs under come from exactly one data source, and
        // there is no sense in which a wildcard could pick it. Refused here rather than at binding so the user is
        // told while the position in the query text is still known. The signature has already established that this
        // argument exists and is a name.
        TableFunctionArgument argument = call.arguments().get(0);
        String dataSourceName = call.name(0);
        if (ExternalSourceAuthorizer.isExactLocalName(dataSourceName) == false) {
            throw new ParsingException(
                argument.source(),
                "[{}] reads one data source, and [{}] is not a single data source name",
                call.name(),
                dataSourceName
            );
        }
    }

    @Override
    public List<String> dataSourceNames(TableFunctionCall call) {
        return List.of(call.name(0));
    }

    @Override
    public LogicalPlan toRelation(TableFunctionCall call, List<NamedExpression> metadataFields, TableFunctionContext context) {
        Bound bound = bind(call, context);
        return new UnresolvedExternalRelation(
            call.source(),
            Literal.keyword(call.source(), bound.dataset().resource()),
            bound.read().config(),
            metadataFields,
            // No dataset name: nothing was registered, so there is none for a node to resolve. What a node resolves
            // instead is the call itself, which rides the plan and comes back here through rebind below.
            null,
            bound.dataset().mapping(),
            false,
            TableFunctionRequest.of(call)
        );
    }

    @Override
    public Read bindOnDataNode(TableFunctionRequest request, TableFunctionContext context) {
        // The signature check ran on the coordinator against the query text. By here the call has crossed the wire,
        // so its shape is re-established rather than assumed, through the same signature that checked it there.
        TableFunctionCall call = request.asCall();
        SIGNATURE.check(call);
        return bind(call, context).read();
    }

    /** What one call binds to against a given project: the dataset it describes and the read that follows. */
    private record Bound(Dataset dataset, Read read) {}

    /**
     * The whole of binding, shared by the coordinator's {@link #toRelation} and the node's
     * {@link #bindOnDataNode} so that one configuration is derived by one piece of code. The data source comes from
     * {@code context}'s project — this coordinator's, or that node's — and never from anything the plan carried.
     */
    private Bound bind(TableFunctionCall call, TableFunctionContext context) {
        String dataSourceName = call.name(0);
        DataSource parent = context.dataSource(dataSourceName);
        if (parent == null) {
            // On the coordinator, reached only when the data source went away between the authorization round-trip
            // and here: a name the caller may read but this project does not have is refused by the resolver first.
            // On a node, this is the refusal for a data source that node has not applied, or no longer has.
            throw new ResourceNotFoundException("data source [{}] not found", dataSourceName);
        }
        DataSourceValidator validator = dataSourceValidators.get(parent.type());
        if (validator == null) {
            throw new IllegalStateException("no validator registered for data source type [" + parent.type() + "]");
        }
        // The resource is its own argument because every call needs one, which is a nicer thing to write than a key
        // in a map. Put back into the definition here, so what the validator sees is the registration API's body and
        // the two routes cannot drift: the hoisting is this function's surface, not a second definition format.
        // Re-assembled into the registration API's body, so the validator and the mapping parser see exactly what a
        // PUT would hand them. The argument list is that body's top level spread across slots; nothing inside any of
        // the chunks is rearranged, which is the point of spreading them rather than wrapping them.
        Map<String, Object> definitionBody = new LinkedHashMap<>();
        definitionBody.put(PutDatasetAction.Request.RESOURCE.getPreferredName(), call.name(1));
        Map<String, Object> options = call.namedValues(2);
        if (options != null) {
            if (options.containsKey(PutDatasetAction.Request.RESOURCE.getPreferredName())) {
                throw new IllegalArgumentException(
                    "[" + NAME + "] takes the resource as its second argument; remove it from the options"
                );
            }
            definitionBody.putAll(options);
        }
        InlineDatasetDefinition definition = InlineDatasetDefinition.fromPayload(definitionBody);
        Dataset dataset = DatasetService.validateDefinition(
            validator,
            parent,
            INLINE_DATASET_NAME,
            dataSourceName,
            definition.resource(),
            definition.description(),
            definition.settings(),
            definition.mapping()
        );
        return new Bound(dataset, new Read(dataset.resource(), DatasetRewriter.datasetConfig(parent, dataset)));
    }
}
