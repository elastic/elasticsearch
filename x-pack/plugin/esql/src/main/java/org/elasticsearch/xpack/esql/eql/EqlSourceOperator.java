/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.NotSerializableExceptionWrapper;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.compute.operator.SourceOperator;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.eql.EqlClientException;
import org.elasticsearch.xpack.eql.action.EqlClusterShards;
import org.elasticsearch.xpack.eql.action.EqlSearchAction;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.eql.action.EqlSearchResponse;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.plan.logical.EqlRelation;
import org.elasticsearch.xpack.ql.ParsingException;
import org.elasticsearch.xpack.ql.QlClientException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Coordinator-local source operator that delegates to the EQL engine: on first poll it issues a single
 * {@code EqlSearchAction} asynchronously, and once the (bounded) response arrives it converts it to one
 * {@link Page} under the {@code EQL} command's fixed schema. The request runs in the caller's security
 * context, so the user's privileges on the EQL target indices apply.
 *
 * <p>The operator is single-shot: it emits at most one page. It never blocks a compute thread — while the
 * search is in flight {@link #isBlocked()} returns a listener the driver waits on, and the async action is
 * tracked on the {@link DriverContext} so the driver is not considered finished prematurely.
 */
public class EqlSourceOperator extends SourceOperator {

    public record Factory(
        Client client,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnDefaultSize,
        EsqlExecutionInfo executionInfo
    ) implements SourceOperatorFactory {
        @Override
        public String describe() {
            return "EqlSourceOperator[mode=" + mode + ", indices=" + String.join(",", request.indices()) + "]";
        }

        @Override
        public SourceOperator get(DriverContext driverContext) {
            return new EqlSourceOperator(driverContext, client, request, mode, schema, source, warnOnDefaultSize, executionInfo);
        }
    }

    private static final int MAX_REPORTED_FAILURES = 5;
    private static final Pattern FOUND_PROBLEMS = Pattern.compile("Found \\d+ problems?\\R");
    private static final Pattern PROBLEM_START = Pattern.compile("^(?=line -?\\d{1,9}:-?\\d{1,9}: )", Pattern.MULTILINE);
    private static final Pattern LEADING_POSITION = Pattern.compile("line (-?\\d{1,9}):(-?\\d{1,9}): ");
    private static final Pattern EQL_POSITION = Pattern.compile("line (\\d+):(\\d+):");

    private final DriverContext driverContext;
    private final Client client;
    private final EqlSearchRequest request;
    private final EqlRelation.Mode mode;
    private final List<Attribute> schema;
    private final Source source;
    private final boolean warnOnDefaultSize;
    private final EsqlExecutionInfo executionInfo;

    private boolean requested;
    private boolean emitted;
    private SubscribableListener<Void> blocked;
    private volatile Page page;
    private volatile Exception failure;
    // Guards the close()-vs-response-callback race: Driver.drainAndCloseOperators runs before waitForAsyncActions,
    // so close() can fire while the EQL search is still in flight. Mutations of page/closed are synchronized(this).
    private boolean closed;

    private EqlSourceOperator(
        DriverContext driverContext,
        Client client,
        EqlSearchRequest request,
        EqlRelation.Mode mode,
        List<Attribute> schema,
        Source source,
        boolean warnOnDefaultSize,
        EsqlExecutionInfo executionInfo
    ) {
        this.driverContext = driverContext;
        this.client = client;
        this.request = request;
        this.mode = mode;
        this.schema = schema;
        this.source = source;
        this.warnOnDefaultSize = warnOnDefaultSize;
        this.executionInfo = executionInfo;
    }

    private void ensureRequested() {
        if (requested) {
            return;
        }
        // A failure that names no cluster comes from the cluster that ran the search. Work it out before the request goes
        // out: the EQL engine strips the cluster from the indices of a search it hands to a single remote cluster.
        String searchedCluster = searchedCluster(request.indices(), executionInfo.clusterAliases());
        requested = true;
        blocked = new SubscribableListener<>();
        driverContext.addAsyncAction();
        // client.execute delivers every outcome — including a synchronous registration failure such as a
        // TaskCancelledException when the parent task is already banned — through the listener, never by throwing
        // (NodeClient.doExecute forwards those to onFailure), so the async action is always released below.
        client.execute(EqlSearchAction.INSTANCE, request, ActionListener.wrap(response -> {
            try {
                // Convert synchronously inside the callback and copy the bytes we need out of the (ref-counted)
                // response into blocks. We do not own a reference here — the EQL transport action delivers the
                // response via respondAndRelease and releases it once this listener returns — so we must not
                // decRef it ourselves (doing so over-releases).
                // This callback runs on the EQL response thread, outside the driver run loop, so the blocks must come from
                // the root factory: the driver's local breaker may only be touched by the driver thread.
                // isPartial() only flags a still-running async response; a completed one reports lost shards in shardFailures().
                recordPartialResults(response, searchedCluster);
                Page built = EqlPageConverter.toPage(response, mode, schema, driverContext.blockFactory().parent());
                synchronized (this) {
                    if (closed) {
                        // The operator was closed (cancellation / sibling-operator failure) before the response
                        // arrived; release the freshly-built page here or its breaker-accounted blocks leak.
                        built.releaseBlocks();
                    } else {
                        page = built;
                    }
                }
            } catch (Exception e) {
                failure = e;
            } finally {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            }
        }, e -> {
            failure = e;
            try {
                failure = located(e);
            } finally {
                driverContext.removeAsyncAction();
                blocked.onResponse(null);
            }
        }));
        maybeWarnDefaultSize();
    }

    @Override
    public IsBlockedResult isBlocked() {
        ensureRequested();
        return blocked.isDone() ? NOT_BLOCKED : new IsBlockedResult(blocked, "eql");
    }

    @Override
    public Page getOutput() {
        ensureRequested();
        if (blocked.isDone() == false) {
            return null;
        }
        if (failure != null) {
            Exception e = failure;
            failure = null;
            throw ExceptionsHelper.convertToRuntime(e);
        }
        if (page != null) {
            Page result = page;
            page = null;
            emitted = true;
            return result;
        }
        return null;
    }

    private Exception located(Exception e) {
        Throwable cause = ExceptionsHelper.unwrapCause(e);
        List<String> problems;
        if (cause instanceof QlClientException clientError) {
            problems = problems(clientError);
        } else if (cause instanceof NotSerializableExceptionWrapper remote && remote.status() == RestStatus.BAD_REQUEST) {
            // A remote cluster's EQL client error arrives as text only: EQL's exceptions are not registered for the wire.
            String prefix = remote.getExceptionName() + ": ";
            String message = remote.getMessage();
            problems = problemsFromText(message.startsWith(prefix) ? message.substring(prefix.length()) : message);
        } else {
            return e;
        }
        String location = source.viewName() == null
            ? Strings.format("line %s:%s: ", source.lineNumber(), source.columnNumber())
            : Strings.format("line %s:%s (in view [%s]): ", source.lineNumber(), source.columnNumber(), source.viewName());
        // Clients such as Kibana read every "line L:C:" as a position in the ES|QL query, so EQL's own must not keep that form.
        problems = problems.stream().map(p -> EQL_POSITION.matcher(p).replaceAll("line $1:$2 of the EQL query:")).toList();
        String reason = problems.size() == 1
            ? ": " + problems.getFirst()
            : problems.stream()
                .map(p -> "\n    " + p.replace("\n", "\n    "))
                .collect(Collectors.joining("", " with " + problems.size() + " problems:", ""));
        VerificationException locatedFailure = new VerificationException(location + "EQL query failed" + reason);
        locatedFailure.addSuppressed(cause);
        return locatedFailure;
    }

    private static List<String> problems(QlClientException cause) {
        return switch (cause) {
            case EqlClientException eql when eql.problems().isEmpty() == false -> eql.problems()
                .stream()
                .map(problem -> inEqlQuery(problem.line(), problem.column(), problem.message()))
                .toList();
            case org.elasticsearch.xpack.eql.parser.ParsingException p -> List.of(
                inEqlQuery(p.getLineNumber(), p.getColumnNumber(), p.getErrorMessage())
            );
            case ParsingException p -> List.of(inEqlQuery(p.getLineNumber(), p.getColumnNumber(), p.getErrorMessage()));
            case QlClientException other -> problemsFromText(Objects.requireNonNullElse(other.getMessage(), ""));
        };
    }

    private static List<String> problemsFromText(String text) {
        Matcher header = FOUND_PROBLEMS.matcher(text);
        if (header.lookingAt() == false) {
            return List.of(problemFromText(text));
        }
        // A problem's message can span lines, so only a line that starts with a position starts the next problem.
        return Arrays.stream(PROBLEM_START.split(text.substring(header.end())))
            .map(problem -> problemFromText(problem.stripTrailing()))
            .toList();
    }

    private static String problemFromText(String problem) {
        Matcher position = LEADING_POSITION.matcher(problem);
        if (position.lookingAt() == false) {
            return problem;
        }
        return inEqlQuery(Integer.parseInt(position.group(1)), Integer.parseInt(position.group(2)), problem.substring(position.end()));
    }

    private static String inEqlQuery(int line, int column, String message) {
        return line > 0 ? "line " + line + ":" + column + " of the EQL query: " + message : message;
    }

    private void maybeWarnDefaultSize() {
        if (warnOnDefaultSize) {
            driverContext.createOnlyWarnings(source)
                .registerWarningWithoutSourceText(
                    "EQL query used the default size of [" + request.size() + "] matches. Set WITH {\"size\": n} to return more."
                );
        }
    }

    private void recordPartialResults(EqlSearchResponse response, String searchedCluster) {
        recordShardCounts(response, searchedCluster);
        if (response.isTimeout()) {
            executionInfo.markPartial();
        }
        List<ShardSearchFailure> failures = response.shardFailures();
        if (failures.isEmpty()) {
            return;
        }
        executionInfo.markPartial();
        Map<String, List<ShardSearchFailure>> failuresByCluster = new HashMap<>();
        for (ShardSearchFailure failure : failures) {
            String cluster = failure.shard() == null || failure.shard().getClusterAlias() == null
                ? searchedCluster
                : failure.shard().getClusterAlias();
            failuresByCluster.computeIfAbsent(cluster, k -> new ArrayList<>()).add(failure);
        }
        boolean mainPlan = executionInfo.isMainPlan();
        failuresByCluster.forEach((cluster, clusterFailures) -> {
            if (executionInfo.getCluster(cluster) != null) {
                executionInfo.swapCluster(cluster, (alias, current) -> withFailures(current, clusterFailures, mainPlan));
            }
        });
    }

    private static EsqlExecutionInfo.Cluster withFailures(
        EsqlExecutionInfo.Cluster cluster,
        List<ShardSearchFailure> failures,
        boolean mainPlan
    ) {
        Set<String> listed = new HashSet<>();
        for (ShardSearchFailure failure : cluster.getFailures()) {
            listed.add(failedShard(failure));
        }
        List<ShardSearchFailure> added = new ArrayList<>();
        for (ShardSearchFailure failure : failures) {
            if (added.size() < MAX_REPORTED_FAILURES && listed.add(failedShard(failure))) {
                added.add(failure);
            }
        }
        EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(cluster).addFailures(added);
        // The main plan only searches a remote cluster that is still RUNNING, so a subplan leaves a remote cluster's status to it.
        boolean local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(cluster.getClusterAlias());
        if ((mainPlan || local)
            && (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING
                || cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)) {
            builder.setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL);
        }
        return builder.build();
    }

    private void recordShardCounts(EqlSearchResponse response, String searchedCluster) {
        List<EqlClusterShards> shards = response.clusterShards();
        if (shards.isEmpty()) {
            return;
        }
        boolean singleLocal = shards.size() == 1 && RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(shards.get(0).clusterAlias());
        boolean recordCounts = executionInfo.isMainPlan() || executionInfo.isSubqueryJoinSubPlan();
        TimeValue took = TimeValue.timeValueMillis(response.took());
        for (EqlClusterShards counts : shards) {
            if (counts.total() == 0 && counts.successful() == 0 && counts.skipped() == 0 && counts.failed() == 0) {
                continue;
            }
            String cluster = singleLocal ? searchedCluster : counts.clusterAlias();
            if (executionInfo.getCluster(cluster) == null) {
                continue;
            }
            executionInfo.swapCluster(cluster, (alias, current) -> {
                EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(current);
                if (recordCounts) {
                    EsqlExecutionInfo.applyShardCounts(
                        builder,
                        current,
                        counts.total(),
                        counts.successful(),
                        counts.skipped(),
                        counts.failed(),
                        true
                    );
                }
                if (current.getTook() != null && executionInfo.clusterReportsSearchTook(alias)) {
                    builder.setTook(TimeValue.timeValueNanos(current.getTook().nanos() + took.nanos()));
                } else {
                    builder.setTook(took);
                }
                if (counts.failed() > 0 && current.getFailures().isEmpty() && response.shardFailures().isEmpty()) {
                    builder.addFailures(
                        List.of(new ShardSearchFailure(new ElasticsearchException("[" + counts.failed() + "] shards failed")))
                    );
                    boolean local = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(alias);
                    if ((executionInfo.isMainPlan() || local)
                        && (current.getStatus() == EsqlExecutionInfo.Cluster.Status.RUNNING
                            || current.getStatus() == EsqlExecutionInfo.Cluster.Status.SUCCESSFUL)) {
                        builder.setStatus(EsqlExecutionInfo.Cluster.Status.PARTIAL);
                    }
                }
                executionInfo.markClusterSearchTook(alias);
                return builder.build();
            });
            if (counts.failed() > 0) {
                executionInfo.markPartial();
            }
        }
    }

    // EQL lists a shard once per search that lost it, and a FORK runs the search once per branch.
    private static String failedShard(ShardSearchFailure failure) {
        return failure.shard() == null ? failure.reason() : failure.shard().getShardId().toString();
    }

    private static String searchedCluster(String[] indices, Set<String> queryClusters) {
        Set<String> clusters = new HashSet<>();
        for (String index : indices) {
            String cluster = RemoteClusterAware.splitIndexName(index).getClusterGroupingKey();
            if (Regex.isSimpleMatchPattern(cluster) == false) {
                clusters.add(cluster);
                continue;
            }
            for (String alias : queryClusters) {
                if (RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY.equals(alias) == false && Regex.simpleMatch(cluster, alias)) {
                    clusters.add(alias);
                }
            }
        }
        return clusters.size() == 1 ? clusters.iterator().next() : RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    }

    @Override
    public boolean isFinished() {
        // Stay "not finished" while a failure is pending so the driver calls getOutput() and the exception propagates.
        return emitted && failure == null;
    }

    @Override
    public void finish() {
        emitted = true;
    }

    @Override
    public void close() {
        synchronized (this) {
            closed = true;
            if (page != null) {
                page.releaseBlocks();
                page = null;
            }
        }
    }
}
