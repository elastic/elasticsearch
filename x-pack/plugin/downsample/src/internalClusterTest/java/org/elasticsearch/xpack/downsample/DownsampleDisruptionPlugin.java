/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilterChain;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.node.Node;
import org.elasticsearch.persistent.StartPersistentTaskAction;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xpack.core.downsample.DownsampleShardTask;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/**
 * Lets disruption tests stop a downsample operation at a chosen point, apply a disruption (node loss, master failover,
 * relocation, network partition, ...) and then let the operation continue. This ensures that the disruption takes effect,
 * so the test would not exercise the recovery path it is meant to cover.
 * <p>
 * It installs an {@link ActionFilter} on every node, so it intercepts actions where they are executed: the shard-level
 * work of a downsample persistent task, and the steps of the downsample action on the elected master. It offers two controls:
 * <ul>
 *     <li>{@link #park}: holds matching executions until {@link #release()} lets them continue or {@link #failParked()} fails
 *     them.</li>
 *     <li>{@link #failNext}: fails a number of matching executions with an exception.</li>
 * </ul>
 * The disruptions are shared by the plugin instances of all nodes, see {@link DisruptionsState}. Tests must call {@link #reset()} when
 * they finish.
 */
public class DownsampleDisruptionPlugin extends Plugin implements ActionPlugin {

    private static final Logger logger = LogManager.getLogger(DownsampleDisruptionPlugin.class);

    /**
     * An execution held back by {@link #park}. If the {@code node} has been stopped or restarted the
     * request is dropped.
     */
    private record Parked(LocalNode node, String action, ActionRequest request, Runnable proceed, Consumer<Exception> fail) {}

    /**
     * Applies to the executions that match {@code matcher}, as long as it has {@code remaining} permits left.
     * A {@code null} {@code remaining} means the rule applies to every matching execution.
     */
    private record Rule(BiPredicate<String, ActionRequest> matcher, @Nullable Semaphore remaining) {
        Rule(BiPredicate<String, ActionRequest> matcher, int times) {
            this(matcher, new Semaphore(times));
        }

        /**
         * @return {@code true} if the rule applies to this execution: it matches and a permit is left.
         */
        boolean tryConsume(String action, ActionRequest request) {
            return matcher.test(action, request) && (remaining == null || remaining.tryAcquire());
        }
    }

    private static class LocalNode {
        private final String name;
        private volatile boolean closed;

        LocalNode(String name) {
            this.name = name;
            closed = false;
        }
    }

    /**
     * The disruptions a test installed: the rules that decide which executions to park or fail, and the executions parked so far.
     * All nodes of an internal test cluster run in the same JVM, so every node's plugin instance shares {@link #DISRUPTIONS_STATE}.
     */
    private static final class DisruptionsState {
        // guarded by this
        private Rule parkRule;
        // guarded by this
        private final List<Parked> parked = new ArrayList<>();
        private volatile Rule failRule;

        synchronized void setParkRule(@Nullable Rule rule) {
            parkRule = rule;
        }

        /**
         * Parks the execution that {@code parkedSupplier} creates if the park rule applies to it.
         * @return {@code true} if the execution was parked
         */
        synchronized boolean tryPark(String action, ActionRequest request, Supplier<Parked> parkedSupplier) {
            if (parkRule != null && parkRule.tryConsume(action, request)) {
                parked.add(parkedSupplier.get());
                return true;
            }
            return false;
        }

        /**
         * Removes the parked executions, optionally stopping parking new ones in the same step.
         * @return the executions that were parked
         */
        synchronized List<Parked> takeParked(boolean stopParking) {
            if (stopParking) {
                parkRule = null;
            }
            final List<Parked> taken = new ArrayList<>(parked);
            parked.clear();
            return taken;
        }

        /**
         * @return the parked executions, except the ones parked on nodes that have been closed since
         */
        synchronized List<Parked> parkedOnOpenNodes() {
            return parked.stream().filter(p -> p.node().closed == false).toList();
        }

        void setFailRule(@Nullable Rule rule) {
            failRule = rule;
        }

        boolean tryFail(String action, ActionRequest request) {
            final Rule rule = failRule;
            return rule != null && rule.tryConsume(action, request);
        }

        int remainingFailures() {
            final Rule rule = failRule;
            return rule == null ? 0 : rule.remaining() == null ? Integer.MAX_VALUE : rule.remaining().availablePermits();
        }
    }

    private static final DisruptionsState DISRUPTIONS_STATE = new DisruptionsState();

    private final LocalNode localNode;
    private volatile ThreadContext threadContext;

    public DownsampleDisruptionPlugin(Settings settings) {
        this.localNode = new LocalNode(Node.NODE_NAME_SETTING.get(settings));
    }

    /**
     * Parks the first {@code times} executions that match {@code matcher}, until {@link #release()} is called.
     */
    public static void park(int times, BiPredicate<String, ActionRequest> matcher) {
        DISRUPTIONS_STATE.setParkRule(new Rule(matcher, times));
    }

    /**
     * Parks every execution that matches {@code matcher}, until {@link #release()} is called. This also parks the
     * executions of persistent tasks that get reassigned while a disruption is in progress.
     */
    public static void parkAll(BiPredicate<String, ActionRequest> matcher) {
        DISRUPTIONS_STATE.setParkRule(new Rule(matcher, null));
    }

    /**
     * Stops parking new executions, but keeps the executions parked so far parked until {@link #release()} is called.
     */
    public static void stopParking() {
        DISRUPTIONS_STATE.setParkRule(null);
    }

    /**
     * Stops parking and lets every parked execution continue, except the ones parked on a node that has been closed since.
     */
    public static void release() {
        stopParking();
        releaseParked();
    }

    /**
     * Lets every parked execution continue, except the ones parked on a node that has been closed since, and keeps parking
     * the executions that match the current {@link #park} rule.
     */
    public static void releaseParked() {
        for (Parked parked : DISRUPTIONS_STATE.takeParked(false)) {
            if (parked.node().closed) {
                logger.info("-> dropping [{}] parked on closed node [{}]", parked.action(), parked.node().name);
            } else {
                logger.info("-> releasing [{}] parked on node [{}]", parked.action(), parked.node().name);
                parked.proceed().run();
            }
        }
    }

    /**
     * Stops parking and fails every parked execution, except the ones parked on a node that has been closed since. This
     * simulates the failure of an execution that was in flight while the test applied a disruption.
     */
    public static void failParked() {
        for (Parked parked : DISRUPTIONS_STATE.takeParked(true)) {
            if (parked.node().closed) {
                logger.info("-> dropping [{}] parked on closed node [{}]", parked.action(), parked.node().name);
            } else {
                logger.info("-> failing [{}] parked on node [{}]", parked.action(), parked.node().name);
                parked.fail()
                    .accept(
                        new ElasticsearchException(
                            "simulated failure of parked [" + parked.action() + "] on node [" + parked.node().name + "]"
                        )
                    );
            }
        }
    }

    /**
     * @return the parked requests, except the ones parked on nodes that have been closed since.
     */
    public static List<ActionRequest> parkedRequests() {
        return DISRUPTIONS_STATE.parkedOnOpenNodes().stream().map(Parked::request).toList();
    }

    /**
     * @return the names of the nodes holding a parked execution, one entry per execution. Executions parked on nodes that
     * have been closed since are not included.
     */
    public static List<String> parkedOnNodes() {
        return DISRUPTIONS_STATE.parkedOnOpenNodes().stream().map(p -> p.node().name).toList();
    }

    /**
     * Fails the first {@code times} executions that match {@code matcher}.
     */
    public static void failNext(int times, BiPredicate<String, ActionRequest> matcher) {
        DISRUPTIONS_STATE.setFailRule(new Rule(matcher, times));
    }

    /**
     * @return the number of executions that {@link #failNext} still has to fail.
     */
    public static int remainingFailures() {
        return DISRUPTIONS_STATE.remainingFailures();
    }

    public static void reset() {
        release();
        DISRUPTIONS_STATE.setFailRule(null);
    }

    /**
     * Matches the executions that {@code matcher} matches and whose zero-based ordinal among those executions is accepted by
     * {@code ordinals}, for example {@code ordinal -> ordinal == 1} for the second one only. It also counts how many executions
     * {@code matcher} matched, so a rule using it must only be installed once. It counts the executions of every node, so it
     * is meant for operations that run on a single node, such as the bulk requests of a single shard-level task.
     */
    public static OrdinalMatcher ordinal(BiPredicate<String, ActionRequest> matcher, IntPredicate ordinals) {
        return new OrdinalMatcher(matcher, ordinals);
    }

    public static final class OrdinalMatcher implements BiPredicate<String, ActionRequest> {
        private final BiPredicate<String, ActionRequest> matcher;
        private final IntPredicate ordinals;
        private final AtomicInteger matched = new AtomicInteger();

        private OrdinalMatcher(BiPredicate<String, ActionRequest> matcher, IntPredicate ordinals) {
            this.matcher = matcher;
            this.ordinals = ordinals;
        }

        @Override
        public boolean test(String action, ActionRequest request) {
            return matcher.test(action, request) && ordinals.test(matched.getAndIncrement());
        }

        /**
         * @return how many executions {@code matcher} matched so far, whether their ordinal was accepted or not
         */
        public int matched() {
            return matched.get();
        }
    }

    /**
     * Matches the shard-level work of the downsample persistent tasks whose source shard belongs to {@code sourceIndex}.
     */
    public static BiPredicate<String, ActionRequest> shardTask(String sourceIndex) {
        return (action, request) -> DownsampleShardPersistentTaskExecutor.DelegatingAction.NAME.equals(action)
            && request instanceof IndicesRequest indicesRequest
            && Arrays.asList(indicesRequest.indices()).contains(sourceIndex);
    }

    /**
     * Matches the starting of the downsample persistent tasks.
     */
    public static BiPredicate<String, ActionRequest> persistentTaskStart() {
        return (action, request) -> StartPersistentTaskAction.INSTANCE.name().equals(action)
            && request instanceof StartPersistentTaskAction.Request start
            && DownsampleShardTask.TASK_NAME.equals(start.getTaskName());
    }

    /**
     * Matches the executions of {@code actionName} that target {@code index}, including bulk requests with an item for it.
     */
    public static BiPredicate<String, ActionRequest> actionOnIndex(String actionName, String index) {
        return (action, request) -> {
            if (actionName.equals(action) == false) {
                return false;
            }
            if (request instanceof BulkRequest bulk) {
                return bulk.requests().stream().anyMatch(r -> index.equals(r.index()));
            }
            return request instanceof IndicesRequest indicesRequest && Arrays.asList(indicesRequest.indices()).contains(index);
        };
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        threadContext = services.threadPool().getThreadContext();
        return List.of();
    }

    @Override
    public void close() {
        localNode.closed = true;
    }

    @Override
    public List<ActionFilter> getActionFilters() {
        return List.of(new ActionFilter() {
            @Override
            public int order() {
                return Integer.MIN_VALUE;
            }

            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void apply(
                Task task,
                String action,
                Request request,
                ActionListener<Response> listener,
                ActionFilterChain<Request, Response> chain
            ) {
                if (DISRUPTIONS_STATE.tryFail(action, request)) {
                    logger.info("-> failing [{}] on node [{}]", action, localNode.name);
                    listener.onFailure(
                        new ElasticsearchException("simulated failure of [" + action + "] on node [" + localNode.name + "]")
                    );
                    return;
                }
                final boolean parked = DISRUPTIONS_STATE.tryPark(action, request, () -> {
                    logger.info("-> parking [{}] on node [{}]", action, localNode.name);
                    // The parked execution is resumed by the test thread, so it has to carry the original thread context
                    final Supplier<ThreadContext.StoredContext> originalContext = threadContext.newRestorableContext(false);
                    return new Parked(localNode, action, request, () -> {
                        try (ThreadContext.StoredContext ignored = originalContext.get()) {
                            chain.proceed(task, action, request, listener);
                        } catch (Exception e) {
                            listener.onFailure(e);
                        }
                    }, e -> {
                        try (ThreadContext.StoredContext ignored = originalContext.get()) {
                            listener.onFailure(e);
                        }
                    });
                });
                if (parked == false) {
                    chain.proceed(task, action, request, listener);
                }
            }
        });
    }
}
