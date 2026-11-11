/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetDefinitionValidator;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunction;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionArgument;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionInvocation;
import org.elasticsearch.xpack.esql.tablefunction.TableFunctionSignature;

import java.util.List;
import java.util.Map;

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
 * It adds no authorization of its own, so a caller who may run the query may describe a dataset against any data
 * source registered in the project. That is why the syntax is confined to snapshot builds: the permission model for
 * a dataset described where it is read is still being designed, and a release node refuses the definition on the
 * wire as well ({@link DatasetConfigResolver#rebind}).
 */
public final class DatasetTableFunction implements TableFunction {

    public static final String NAME = "dataset";

    /** The definition field the shorthand form supplies on its own. */
    private static final String RESOURCE_FIELD = "resource";

    private final DatasetDefinitionValidator definitionValidator;

    public DatasetTableFunction(DatasetDefinitionValidator definitionValidator) {
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
            TableFunctionSignature.either("definition", TableFunctionSignature.Kind.NAME, TableFunctionSignature.Kind.NAMED_VALUES)
        );
    }

    /**
     * The data source is one registered name, not a way of asking for several: the reading node looks the name up
     * in its own registry exactly, so a pattern, comma list, remote prefix or exclusion would resolve to nothing at
     * all rather than to the several sources it looks like it asks for. The definition itself is checked by the
     * validator that holds a registration to it.
     */
    @Override
    public void validate(TableFunctionInvocation call) {
        String dataSource = call.name(0);
        if (ExternalSourceAuthorizer.isExactLocalName(dataSource) == false) {
            throw new ParsingException(call.arguments().get(0).source(), "[{}] reads one data source, found [{}]", call.name(), dataSource);
        }
    }

    /** Nothing here needs I/O, so the listener completes on the caller's thread, which is the planning pool. */
    @Override
    public void toRelation(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        ActionListener<LeafPlan> listener
    ) {
        DatasetReference.Inline inline = new DatasetReference.Inline(call.name(0), definitionOf(call));
        ActionListener.completeWith(listener, () -> bind(call, metadataFields, projectMetadata, inline));
    }

    private LeafPlan bind(
        TableFunctionInvocation call,
        List<NamedExpression> metadataFields,
        ProjectMetadata projectMetadata,
        DatasetReference.Inline inline
    ) {
        DatasetDefinitionValidator.BoundDataset bound = definitionValidator.validateInline(projectMetadata, inline);
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
