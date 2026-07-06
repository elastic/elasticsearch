/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A request to execute an index-abstraction (a view or a dataset) by <b>name</b> on its home cluster — the execution half
 * of ES|QL federation, the sibling of the schema half ({@code resolve_schema} via {@code TransportResolveSchemaAction}).
 *
 * <p>Unlike {@link ClusterComputeRequest}, this request carries <b>no plan and no query text</b>: only the abstraction's
 * identity ({@link #abstractionName}). The home cluster resolves that name through its <em>own</em> kind-blind
 * {@code SchemaService} umbrella ({@code resolvePlan}), plans the resolved body with its own planning pipeline, runs it
 * through the existing compute path, and sinks the result pages into an exchange sink identified by {@link #sessionId}.
 * The exchange must have been opened via {@link ExchangeService#openExchange} before sending this request; the coordinator
 * on the querying cluster polls pages from that sink — reusing the exact exchange result-transport substrate
 * {@link ClusterComputeRequest} uses. The same action serves both a remote view and a remote dataset (kind-blind).
 *
 * <p>Because execution re-resolves the name on the home cluster at a different time than the coordinator resolved its
 * schema, the request also carries {@link #expectedAttributes} — the ordered output schema the coordinator resolved
 * against. The home handler validates its freshly-resolved plan's output against this expectation (same names, types,
 * order) and fails loud on drift, closing the name-based TOCTOU that could otherwise return column-swapped pages
 * positionally into the coordinator's layout. Exchange pages carry no schema header, so this is the contract that keeps
 * name-based execution correct.
 *
 * <p>This request is an {@link IndicesRequest.Replaceable} — mirroring {@link ClusterComputeRequest} and
 * {@code TransportResolveSchemaAction.Request} — so the security action-filter authorizes {@link #abstractionName}
 * before the {@code indices:}-scoped handler runs it.
 */
final class ExecuteAbstractionRequest extends AbstractTransportRequest implements IndicesRequest.Replaceable {

    /**
     * Marks the transport version in which the {@code execute_abstraction} action and this request type first appear. The
     * request is entirely new wire — an older remote has no handler for it — so the coordinator dispatch
     * ({@code FederationExecutionService.fetchAbstraction}) gates on this: it refuses to send to a connection whose
     * negotiated version does not {@link TransportVersion#supports} it, failing loud instead of hitting an
     * unknown-action rejection. There is no BWC-conditional field inside {@code writeTo}/the reader: the whole request is
     * version-gated at the send site.
     */
    static final TransportVersion ESQL_EXECUTE_ABSTRACTION = TransportVersion.fromName("esql_execute_abstraction");

    /**
     * Index options mirroring {@code TransportResolveSchemaAction.Request.SCHEMA_INDICES_OPTIONS}: the abstraction name
     * must resolve through the view/dataset abstractions during security filtering, exactly as the schema half does.
     */
    private static final IndicesOptions ABSTRACTION_INDICES_OPTIONS = IndicesOptions.builder()
        .wildcardOptions(IndicesOptions.WildcardOptions.builder().allowEmptyExpressions(true))
        .indexAbstractionOptions(IndicesOptions.IndexAbstractionOptions.builder().resolveDatasets(true).resolveViews(true).build())
        .concreteTargetOptions(IndicesOptions.ConcreteTargetOptions.ALLOW_UNAVAILABLE_TARGETS)
        .build();

    private final String clusterAlias;
    private final String sessionId;
    private final Configuration configuration;
    private final String abstractionName;
    private final List<Attribute> expectedAttributes;

    private transient String[] indices;

    /**
     * @param clusterAlias       the cluster alias of the querying (coordinator) cluster, as seen from the home cluster
     * @param sessionId          the sessionId of the exchange sink the home cluster places its output pages into
     * @param configuration      the coordinator's configuration; today it is only the {@code PlanStreamOutput} context
     *                           for (de)serializing {@link #expectedAttributes}. The home session synthesizes its own
     *                           configuration from the {@code FROM <name>} request, so timezone/locale/pragma propagation
     *                           from the coordinator is a follow-up.
     * @param abstractionName    the identity (name) of the view or dataset to resolve and run on the home cluster
     * @param expectedAttributes the ordered output schema the coordinator resolved against; the home handler validates
     *                           its freshly-resolved plan's output against this and fails loud on drift (schema-drift guard)
     */
    ExecuteAbstractionRequest(
        String clusterAlias,
        String sessionId,
        Configuration configuration,
        String abstractionName,
        List<Attribute> expectedAttributes
    ) {
        this.clusterAlias = clusterAlias;
        this.sessionId = sessionId;
        this.configuration = configuration;
        this.abstractionName = abstractionName;
        this.expectedAttributes = expectedAttributes;
        this.indices = new String[] { abstractionName };
    }

    ExecuteAbstractionRequest(StreamInput in) throws IOException {
        this(in, null);
    }

    /**
     * @param idMapper must always be null in production. Only used in tests to remap NameIds when deserializing, so the
     *                 round-tripped attributes compare equal to the originals (mirrors {@link ClusterComputeRequest}).
     */
    ExecuteAbstractionRequest(StreamInput in, PlanStreamInput.NameIdMapper idMapper) throws IOException {
        super(in);
        this.clusterAlias = in.readString();
        this.sessionId = in.readString();
        this.configuration = new Configuration(
            // TODO make EsqlConfiguration Releasable
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
        this.abstractionName = in.readString();
        // Attributes serialize through a PlanStreamInput (their writeTo/read cast to it for the attribute cache);
        // mirror ClusterComputeRequest, which wraps the raw stream to (de)serialize its plan.
        this.expectedAttributes = new PlanStreamInput(in, in.namedWriteableRegistry(), configuration, idMapper)
            .readNamedWriteableCollectionAsList(Attribute.class);
        this.indices = new String[] { abstractionName };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterAlias);
        out.writeString(sessionId);
        configuration.writeTo(out);
        out.writeString(abstractionName);
        new PlanStreamOutput(out, configuration).writeNamedWriteableCollection(expectedAttributes);
    }

    @Override
    public String[] indices() {
        return indices;
    }

    @Override
    public IndicesRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        return ABSTRACTION_INDICES_OPTIONS;
    }

    @Override
    public boolean allowsRemoteIndices() {
        return true;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        if (parentTaskId.isSet() == false) {
            assert false : "ExecuteAbstractionRequest must have a parent task";
            throw new IllegalStateException("ExecuteAbstractionRequest must have a parent task");
        }
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public String getDescription() {
                return ExecuteAbstractionRequest.this.getDescription();
            }
        };
    }

    String clusterAlias() {
        return clusterAlias;
    }

    String sessionId() {
        return sessionId;
    }

    Configuration configuration() {
        return configuration;
    }

    String abstractionName() {
        return abstractionName;
    }

    List<Attribute> expectedAttributes() {
        return expectedAttributes;
    }

    @Override
    public String getDescription() {
        return "abstraction=" + abstractionName;
    }

    @Override
    public String toString() {
        return "ExecuteAbstractionRequest{" + getDescription() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExecuteAbstractionRequest request = (ExecuteAbstractionRequest) o;
        return clusterAlias.equals(request.clusterAlias)
            && sessionId.equals(request.sessionId)
            && configuration.equals(request.configuration)
            && abstractionName.equals(request.abstractionName)
            && expectedAttributes.equals(request.expectedAttributes)
            && getParentTask().equals(request.getParentTask());
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, sessionId, configuration, abstractionName, expectedAttributes);
    }
}
