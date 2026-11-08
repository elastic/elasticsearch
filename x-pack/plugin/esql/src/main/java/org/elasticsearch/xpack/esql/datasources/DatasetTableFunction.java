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
 * It denotes exactly what {@code FROM <dataset>} denotes — an {@link UnresolvedExternalRelation}, which the ordinary
 * analyzer resolves by listing the resource and reading its schema, as it does for a registered one. The two differ
 * in one thing: where the definition came from. A registered read names it and every node looks it up; this one
 * carries it ({@link DatasetReference.Inline}) and every node binds it against its own data source registry. Nothing
 * downstream can tell the two apart beyond that, which is the point — an index pattern could become one of these
 * without anything moving.
 *
 * <h2>The definition</h2>
 *
 * The second argument is a dataset definition in the shape a registration body has: {@code resource},
 * {@code settings}, {@code mappings}, {@code description}. A definition whose only field is its resource may be
 * written as that resource alone, so {@code DATASET(s3, "s3://bucket/*.parquet")} and
 * {@code DATASET(s3, {"resource": "s3://bucket/*.parquet"})} are one read. It goes through the validator a
 * registration goes through ({@link DatasetDefinitionValidator}), so whatever a registered dataset can express this
 * can, and whatever a registration refuses this refuses in the same words.
 *
 * <h2>The two privileges</h2>
 *
 * This read names one object, the data source, and it is authorized both ways that object is governed: index
 * {@code read} on the name, and {@code read} on the source under {@code global.data_source}. Both are settled by
 * {@link EsqlAuthorizeDatasourceAction}, issued here as the caller before any definition is bound, and again by a
 * node that receives a plan carrying a definition ({@link ExternalSourceAuthorizer}), because a plan arriving over
 * the wire was not necessarily built by a coordinator the caller was allowed to use.
 * <p>
 * <b>This is not the set a registration asks for, and the difference is open rather than settled.</b> The
 * {@code global.data_source} half is the same one: {@code PutDatasetAction} advertises the same datasource cluster
 * action, which {@code ConfigurableClusterPrivileges} maps to {@code read}. The index half is not. Registering a
 * dataset is {@code indices:admin/esql/dataset/put} against the <i>dataset's</i> name, reachable only from
 * {@code create_dataset}, {@code manage_dataset}, {@code manage} or {@code all}; this action is
 * {@code indices:data/read/esql/authorize_datasource} against the <i>data source's</i> name, which plain
 * {@code read} covers. So a caller holding index {@code read} broadly and {@code read} on one source can read
 * inline what they could not have registered, and the effective gate for them is the {@code global.data_source}
 * half alone. Whether an inline read should instead require the dataset-creation privilege on that name is a policy
 * question that has not been answered.
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
     * The data source is one registered name, not a way of asking for several: a pattern, a comma list, a
     * remote-cluster prefix and an exclusion each name something no data source can be registered as, and admitting
     * any of them would make the read's identity — what both privileges are checked against — plural. Settled here
     * rather than after binding, because it is the name the request to authorize is built from.
     * <p>
     * Nothing is checked about the definition here. It is held to what a registration is held to, by the validator
     * that holds a registration to it, and only once the caller has been authorized to use the source at all.
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
        // The binding runs after the hop, on the search pool the rest of planning runs on: the authorization can
        // complete on another thread (role resolution, a cluster-state wait), as DatasetResolver's does. Metered
        // inside the fork, so the validation the binding does is charged to the query's planning CPU rather than to
        // whichever transport thread answered — a table function holds no tracker of its own to charge it to.
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
