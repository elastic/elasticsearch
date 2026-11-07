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
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;

import java.util.List;

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
public final class DatasetTableFunction implements TableFunction {

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

    @Override
    public void validate(TableFunctionCall call) {
        if (call.arguments().size() != 1) {
            throw new ParsingException(call.source(), "[{}] takes one data source name, found [{}]", call.name(), call.arguments().size());
        }
        if (call.payload() == null) {
            throw new ParsingException(call.source(), "[{}] requires a dataset definition", call.name());
        }
        TableFunctionArgument argument = call.arguments().get(0);
        // One data source, never a pattern: the credentials a read runs under come from exactly one definition, and
        // there is no sense in which a wildcard could pick it. Refused here rather than at binding so the user is
        // told while the position in the query text is still known.
        if (ExternalSourceAuthorizer.isExactLocalName(argument.value()) == false) {
            throw new ParsingException(
                argument.source(),
                "[{}] reads one data source, and [{}] is not a single data source name",
                call.name(),
                argument.value()
            );
        }
    }

    @Override
    public List<String> dataSourceNames(TableFunctionCall call) {
        return List.of(call.arguments().get(0).value());
    }

    @Override
    public LogicalPlan toRelation(TableFunctionCall call, List<NamedExpression> metadataFields, TableFunctionContext context) {
        String dataSourceName = call.arguments().get(0).value();
        DataSource parent = context.dataSource(dataSourceName);
        if (parent == null) {
            // Reached only when the data source went away between the authorization round-trip and here, since a name
            // the caller may read but this project does not have is refused by the resolver before binding starts.
            throw new ResourceNotFoundException("data source [{}] not found", dataSourceName);
        }
        DataSourceValidator validator = context.validator(parent.type());
        if (validator == null) {
            throw new IllegalStateException("no validator registered for data source type [" + parent.type() + "]");
        }
        InlineDatasetDefinition definition = InlineDatasetDefinition.fromPayload(call.payload());
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
        Literal path = Literal.keyword(call.source(), dataset.resource());
        return new UnresolvedExternalRelation(
            call.source(),
            path,
            DatasetRewriter.datasetConfig(parent, dataset),
            metadataFields,
            // No dataset name: nothing was registered. A node reads this leaf as unnamed, which it admits only on a
            // snapshot build — the same footing the inline EXTERNAL command is on, and why the parser rule for a
            // table function is dev-gated. Carrying the data source as a typed field on the plan, so a node can
            // authorize it for itself, is what has to land before that gate can come off.
            null,
            dataset.mapping()
        );
    }
}
