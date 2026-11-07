/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.tablefunction;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A table-function call as the user wrote it, in the form that travels to a data node.
 * <p>
 * The node needs this because it does not trust a plan's configuration. For {@code FROM <dataset>} the plan carries
 * the dataset's name and the node rebuilds the configuration from its own cluster state, which is what stops a
 * crafted plan choosing credentials, a role or an endpoint. A table function has no registered name to play that
 * part, so the call travels instead and the node derives the configuration from it — the same input the coordinator
 * had, resolved against the node's own state by the same code.
 * <p>
 * <b>One type for every table function, on purpose.</b> The alternative was a {@code NamedWriteable} per function,
 * following {@code ExternalSplit}. That is more strongly typed and worse here on all three counts that matter:
 * it makes adding a function require a registration in {@code EsqlPlugin} as well as an implementation; it turns an
 * unknown function on an older node into a deserialization failure rather than the ordinary
 * transport-version-and-capability story; and it earns nothing, because what travels is in every case the name, the
 * positional arguments and the payload — which is all the grammar can produce. So the shape here is the grammar's
 * shape, and a new function rides it without touching the wire.
 * <p>
 * {@link org.elasticsearch.xpack.esql.core.tree.Source} is deliberately absent: it locates the call in the query
 * text for a diagnostic the coordinator raises, and by the time this reaches a node the call has already parsed and
 * validated.
 *
 * @param functionName the function, as {@link TableFunction#name()} reports it. The receiving node resolves it
 *                     against {@link TableFunctionRegistry}, which is built at startup and identical on every node.
 * @param arguments the call's arguments, in the order written — names, and a trailing map when one was given
 */
public record TableFunctionRequest(String functionName, List<TableFunctionArgument> arguments) implements Writeable {

    public TableFunctionRequest {
        Objects.requireNonNull(functionName, "functionName");
        arguments = List.copyOf(arguments);
    }

    /** The wire form of {@code call}: everything a node needs, and nothing that only the query text explains. */
    public static TableFunctionRequest of(TableFunctionInvocation call) {
        return new TableFunctionRequest(call.name(), call.arguments());
    }

    /** The call this request stands for, with every argument's position in the query text gone. */
    public TableFunctionInvocation asCall() {
        return new TableFunctionInvocation(Source.EMPTY, functionName, arguments);
    }

    public static TableFunctionRequest readFrom(StreamInput in) throws IOException {
        return new TableFunctionRequest(in.readString(), in.readCollectionAsList(TableFunctionArgument::readFrom));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(functionName);
        out.writeCollection(arguments);
    }
}
