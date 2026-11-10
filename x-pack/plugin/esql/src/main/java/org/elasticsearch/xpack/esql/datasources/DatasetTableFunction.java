/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlAuthorizeDatasourceAction;
import org.elasticsearch.xpack.esql.action.PlanningCpuTracker;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetDefinitionValidator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionContext;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.rest.RestUtils.REST_MASTER_TIMEOUT_DEFAULT;

/**
 * {@code DATASET(<data source>, <definition>)}: a dataset read written where it is read, without registering the
 * dataset first.
 * <p>
 * It denotes exactly what {@code FROM <dataset>} denotes — an {@link UnresolvedExternalRelation}, resolved by the
 * ordinary analyzer. The two differ in where the definition came from: a registered read names it and every node
 * looks it up, this one carries it ({@link DatasetReference.Inline}) and every node binds it against its own data
 * source registry.
 * <p>
 * The second argument is a dataset definition in the shape a registration body has. A definition whose only field is
 * its resource may be written as that resource alone. It goes through the validator a registration goes through
 * ({@link DatasetDefinitionValidator}), so whatever a registration refuses this refuses in the same words.
 * <p>
 * It names one object, the data source, and is authorized both ways that object is governed — the dataset-creation
 * privilege on the name, and {@code read} on the source under {@code global.data_source} — because describing a
 * dataset against a source is the same act whether or not the description is kept. Both are settled by
 * {@link EsqlAuthorizeDatasourceAction}, here and again on a node receiving the plan
 * ({@link ExternalSourceAuthorizer}). One asymmetry is left, and it is a missing name rather than a weaker check: a
 * registration is authorized against the <i>dataset's</i> name, and an inline read, having none, against the
 * <i>data source's</i>.
 */
public final class DatasetTableFunction implements TableFunction {

    public static final String NAME = "dataset";

    /** The definition field the shorthand form supplies on its own. */
    private static final String RESOURCE_FIELD = "resource";

    private final Client client;
    private final DatasetDefinitionValidator definitionValidator;

    public DatasetTableFunction(Client client, DatasetDefinitionValidator definitionValidator) {
        this.client = client;
        this.definitionValidator = definitionValidator;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public TableFunctionSignature signature() {
        return TableFunctionSignature.of(
            TableFunctionSignature.name("data source"),
            TableFunctionSignature.nameOrNamedValues("definition")
        );
    }

    /**
     * The data source is one registered name, not a way of asking for several: admitting a pattern, comma list,
     * remote prefix or exclusion would make the read's identity — what both privileges are checked against — plural.
     * The definition is checked by the validator that holds a registration to it, once the caller is authorized.
     */
    @Override
    public void validate(TableFunctionInvocation call) {
        String dataSource = call.name(0);
        if (ExternalSourceAuthorizer.isExactLocalName(dataSource) == false) {
            throw new ParsingException(call.arguments().get(0).source(), "[{}] reads one data source, found [{}]", call.name(), dataSource);
        }
    }

    @Override
    public void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        TableFunctionContext context,
        ActionListener<LeafPlan> listener
    ) {
        DatasetReference.Inline inline = new DatasetReference.Inline(call.name(0), definitionOf(call));
        // The binding runs on the search pool rather than on whichever transport thread answered the authorization,
        // as DatasetResolver's does, so its validation is charged to the query's planning CPU.
        ActionListener<ActionResponse.Empty> authorized = new ThreadedActionListener<>(
            client.threadPool().executor(ThreadPool.Names.SEARCH),
            true,
            PlanningCpuTracker.inheritMeteredCpu(listener.map(ignored -> bind(call, metadataFields, context, inline)))
        );
        client.execute(
            EsqlAuthorizeDatasourceAction.TYPE,
            new EsqlAuthorizeDatasourceAction.Request(REST_MASTER_TIMEOUT_DEFAULT, inline.dataSource()),
            authorized
        );
    }

    private LeafPlan bind(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        TableFunctionContext context,
        DatasetReference.Inline inline
    ) {
        DatasetDefinitionValidator.BoundDataset bound = definitionValidator.validateInline(context.projectMetadata(), inline);
        return new UnresolvedExternalRelation(
            call.source(),
            Literal.keyword(call.source(), bound.dataset().resource()),
            DatasetRewriter.datasetConfig(bound.dataSource(), bound.dataset(), null),
            metadataFields,
            inline,
            bound.dataset().mapping()
        );
    }

    /** The definition the call supplied, with a resource written on its own expanded into the definition it means. */
    private static Map<String, Object> definitionOf(TableFunctionInvocation call) {
        TableFunctionArgument argument = call.arguments().get(1);
        if (argument instanceof TableFunctionArgument.Name resource) {
            return Map.of(RESOURCE_FIELD, resource.value());
        }
        return ((TableFunctionArgument.NamedValues) argument).values();
    }
}
