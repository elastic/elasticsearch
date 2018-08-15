package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.MembershipAction.JoinCallback;
import org.elasticsearch.discovery.zen.NodeJoinController;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.LongSupplier;

//TODO: add a test that sends random joins and checks if the join led to success (the fake masterservice fully manages state)
// i.e. similar to NodeJoinControllerTests
public abstract class JoinHelper extends AbstractComponent {

    public static final String JOIN_ACTION_NAME = "internal:discovery/zen2/join";

    // similar to NodeJoinController.ElectionContext.joinRequestAccumulator, captures joins on election
    private final Map<DiscoveryNode, JoinCallback> joinRequestAccumulator = new HashMap<>();

    private final NodeJoinController.JoinTaskExecutor joinTaskExecutor;
    private final MasterService masterService;

    private final TransportService transportService;

    JoinHelper(Settings settings, AllocationService allocationService, MasterService masterService, TransportService transportService,
               LongSupplier currentTermSupplier) {
        super(settings);

        this.masterService = masterService;
        this.transportService = transportService;

        // disable minimum_master_nodes check
        final ElectMasterService electMasterService = new ElectMasterService(settings) {

            @Override
            public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
                return true;
            }

            @Override
            public void logMinimumMasterNodesWarningIfNecessary(ClusterState oldState, ClusterState newState) {
                // ignore
            }

        };

        // reuse JoinTaskExecutor implementation from ZenDiscovery, but hack some checks
        this.joinTaskExecutor = new NodeJoinController.JoinTaskExecutor(allocationService, electMasterService, logger) {

            @Override
            public ClusterTasksResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm = currentTermSupplier.getAsLong();
                if (currentState.term() != currentTerm) {
                    currentState = ClusterState.builder(currentState).term(currentTerm).build();
                }
                return super.execute(currentState, joiningNodes);
            }

        };

        transportService.registerRequestHandler(JOIN_ACTION_NAME, ThreadPool.Names.GENERIC, false, false,
            JoinRequest::new,
            (request, channel, task) -> handleJoinRequest(request, new JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(TransportResponse.Empty.INSTANCE);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send back failure on join request", inner);
                    }
                }
            }));
    }

    public abstract void handleJoinRequest(JoinRequest joinRequest, JoinCallback joinCallback);

    // TODO: maybe add this method in a follow-up.
    public void sendOptionalJoin(DiscoveryNode destination, Optional<Join> optionalJoin) {
        // No synchronisation required
        transportService.sendRequest(destination, JOIN_ACTION_NAME, new JoinRequest(transportService.getLocalNode(), optionalJoin),
            new TransportResponseHandler<TransportResponse.Empty>() {
                @Override
                public TransportResponse.Empty read(StreamInput in) {
                    return TransportResponse.Empty.INSTANCE;
                }

                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    // No synchronisation required
                    logger.debug("SendJoinResponseHandler: successfully joined {}", destination);
                }

                @Override
                public void handleException(TransportException exp) {
                    // No synchronisation required
                    if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                        logger.debug("SendJoinResponseHandler: [{}] failed: {}", destination, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("SendJoinResponseHandler: [{}] failed", destination), exp);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.GENERIC;
                }
            });
    }

    public void addJoinCallback(JoinRequest joinRequest, JoinCallback joinCallback) {
        JoinCallback prev = joinRequestAccumulator.put(joinRequest.getSourceNode(), joinCallback);
        if (prev != null) {
            prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + joinRequest.getSourceNode()));
        }
    }

    public void removeAndFail(JoinRequest joinRequest, CoordinationStateRejectedException exception) {
        final JoinCallback callback = joinRequestAccumulator.remove(joinRequest.getSourceNode());
        if (callback == null) {
            logger.trace("handleJoinRequestUnderLock: submitted task to master service, but vote was rejected", exception);
        } else {
            callback.onFailure(exception);
        }
    }

    public void clearJoins() {
        joinRequestAccumulator.values().forEach(
            joinCallback -> joinCallback.onFailure(new CoordinationStateRejectedException("node stepped down as leader")));
        joinRequestAccumulator.clear();
    }

    public void clearAndSendJoins() {
        final Map<DiscoveryNode, ClusterStateTaskListener> pendingAsTasks = new HashMap<>();
        joinRequestAccumulator.forEach((key, value) -> pendingAsTasks.put(key,
            new NodeJoinController.JoinTaskListener(value, logger)));
        joinRequestAccumulator.clear();

        final String source = "zen-disco-elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";
        // noop listener, the election finished listener determines result
        pendingAsTasks.put(NodeJoinController.BECOME_MASTER_TASK, (source1, e) -> {
        });
        // TODO: should we take any further action when FINISH_ELECTION_TASK fails?
        pendingAsTasks.put(NodeJoinController.FINISH_ELECTION_TASK, (source1, e) -> {
        });

        masterService.submitStateUpdateTasks(source, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
            joinTaskExecutor);
    }

    public void joinLeader(JoinRequest joinRequest, JoinCallback joinCallback) {
        // submit as cluster state update task
        masterService.submitStateUpdateTask("zen-disco-node-join",
            joinRequest.getSourceNode(), ClusterStateTaskConfig.build(Priority.URGENT),
            joinTaskExecutor, new NodeJoinController.JoinTaskListener(joinCallback, logger));
    }

}
