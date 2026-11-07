/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.Build;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.metadata.Dataset;
import org.elasticsearch.cluster.metadata.DatasetMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.esql.datasources.dataset.DatasetDefinitionValidator;
import org.elasticsearch.xpack.esql.datasources.glob.GlobExpander;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSource;
import org.elasticsearch.xpack.esql.datasources.metadata.DataSourceMetadata;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.plan.physical.ExternalSourceExec;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Rebinds an {@link ExternalSourceExec} that arrived with a plan to the dataset definition registered in this node's own
 * cluster state, before any local planning rule or operator reads the exec's config.
 * <p>
 * A plan reaches a node over the transport layer, and the node has no way to tell a plan its own coordinator built from
 * one a holder of a cross-cluster credential crafted. Until here the exec's {@code config} (storage credentials, auth
 * mode, endpoint) and {@code sourcePath} were taken from that plan as they came, which let a foreign plan choose which
 * identity the node reads with. The exec names its dataset, so the node looks that name up in its own registry and
 * replaces both with what the definition says, discarding whatever the plan carried. What the plan still decides is
 * row shaping only: projection, declared renames, byte ranges within the dataset's files.
 * <p>
 * Refusals, all {@code 400}. The binding failures share one message, so a caller probing names learns nothing from
 * the shape of the error:
 * <ul>
 *   <li>a null dataset name on a release build. Only the inline {@code EXTERNAL} command, which exists on snapshot
 *       builds alone, plans a source with no dataset behind it; on a release build such an exec can only have come
 *       from a crafted plan or from a coordinator too old to serialize the name, and the release branch must never fall
 *       back to the plan config, since a client can negotiate an older transport version on purpose.</li>
 *   <li>a dataset that is not registered here, whose data source is not, or whose resource this node cannot parse.</li>
 * </ul>
 * A split outside the dataset's resource is refused naming the split: splits are plan-carried too, and with the
 * registered credentials in hand they could otherwise name any object those credentials reach.
 * A definition edited between the coordinator's planning and this node's execution is refused with a distinct message:
 * split discovery, schema and statistics were computed under the plan's definition, and reading under another can
 * mis-parse. The {@link DefinitionVersion} the plan carries makes that detectable; its pre-image holds no secret value,
 * so this is a consistency check, not a security one. There is no server-side retry; the query fails and is re-run.
 */
public final class DatasetConfigResolver {

    private record ResourcePath(StoragePath path, @Nullable Predicate<String> globMatcher) {}

    private final ProjectMetadata project;
    private final boolean snapshotBuild;
    private final DatasetDefinitionValidator definitionValidator;

    /**
     * @param project       this node's view of the project the request runs in; the same instance the local planner
     *                      is handed, so both see one state
     * @param snapshotBuild whether a source with no dataset name is allowed through unchanged (inline {@code EXTERNAL});
     *                      injected rather than read from {@link Build} so unit tests that plan bare execs keep passing
     *                      when the suite runs against a release build
     */
    public DatasetConfigResolver(ProjectMetadata project, boolean snapshotBuild) {
        this(project, snapshotBuild, new DatasetDefinitionValidator(Map.of()));
    }

    /**
     * @param definitionValidator the one validator a registration goes through, so a definition the plan carried is
     *                            held to what a registered one is held to — including the checks that stop a dataset
     *                            setting shadowing one of the data source's secrets
     */
    public DatasetConfigResolver(ProjectMetadata project, boolean snapshotBuild, DatasetDefinitionValidator definitionValidator) {
        this.project = Objects.requireNonNull(project, "project");
        this.snapshotBuild = snapshotBuild;
        this.definitionValidator = Objects.requireNonNull(definitionValidator, "definitionValidator");
    }

    public static DatasetConfigResolver forProduction(ProjectMetadata project, DatasetDefinitionValidator definitionValidator) {
        return new DatasetConfigResolver(project, Build.current().isSnapshot(), definitionValidator);
    }

    /** Whether a source with no registered dataset behind it is allowed through (snapshot builds only). */
    public boolean snapshotBuild() {
        return snapshotBuild;
    }

    /**
     * Returns {@code exec} reading, under the configuration this node computed, the resource of whichever dataset its
     * {@link DatasetReference} names — one registered here, or one the plan supplied a definition for. Idempotent: a
     * rebound exec carries the recomputed version and the registry map, so a second pass changes nothing.
     * <p>
     * The refusals are decided against {@code project} as this node has applied it. A caller whose applied cluster state
     * is older than the one the plan was built from must not surface a binding refusal as the {@code 400}: the dataset may
     * well be registered in a state this node has not applied yet, and the caller is the one that knows the versions.
     * See the external-worker readiness handling in {@code DataNodeComputeHandler}.
     */
    public ExternalSourceExec rebind(ExternalSourceExec exec) {
        DatasetReference reference = exec.datasetReference();
        if (reference == null) {
            if (snapshotBuild) {
                return exec;
            }
            throw notRegistered(null);
        }
        Resolved resolved = reference instanceof DatasetReference.Named named
            ? resolveRegistered(named.name())
            : resolveSupplied((DatasetReference.Inline) reference);
        return bind(exec, reference.describe(), resolved);
    }

    /** A definition this node resolved for itself, and the data source whose credentials read it. */
    private record Resolved(DataSource dataSource, Dataset dataset) {}

    /** The definition registered here under {@code name}. */
    private Resolved resolveRegistered(String name) {
        Dataset dataset = DatasetMetadata.get(project).get(name);
        DataSource parent = dataset == null ? null : DataSourceMetadata.get(project).get(dataset.dataSource().getName());
        if (dataset == null || parent == null) {
            throw notRegistered(name);
        }
        return new Resolved(parent, dataset);
    }

    /**
     * The definition the plan supplied, re-validated here against this node's own data source. What the plan
     * supplied is the definition — user input, carrying no privilege — while the credentials stay in this node's
     * registry, so a crafted plan can describe a read and still not choose an identity to make it with. It goes
     * through the validator a registration goes through, which is also what stops a setting shadowing one of that
     * source's secrets.
     * <p>
     * Every refusal is a {@code 400} {@link ElasticsearchStatusException}, including the ones the validator raises as
     * a {@link ResourceNotFoundException} or an {@link IllegalStateException}: a data source registered in a state
     * this node has not applied yet looks exactly like one that does not exist, and
     * {@code DataNodeComputeHandler.isDatasetRefusalWhileBehind} can only wait for the state to arrive on a refusal
     * of that shape.
     */
    private Resolved resolveSupplied(DatasetReference.Inline inline) {
        try {
            DatasetDefinitionValidator.BoundDataset bound = definitionValidator.validateInline(project, inline);
            return new Resolved(bound.dataSource(), bound.dataset());
        } catch (ElasticsearchStatusException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new ElasticsearchStatusException(
                "external data source [{}] cannot be read on this node: {}",
                RestStatus.BAD_REQUEST,
                e,
                inline.dataSource(),
                e.getMessage()
            );
        }
    }

    /**
     * What both arms do once resolved, and the reason they share it: a check added here has to cover a read however
     * its dataset was named, and two copies is how one of them would stop.
     */
    private ExternalSourceExec bind(ExternalSourceExec exec, String name, Resolved resolved) {
        Map<String, Object> config = DatasetRewriter.datasetConfig(resolved.dataSource(), resolved.dataset());
        Object planVersion = exec.config().get(DefinitionVersion.CONFIG_KEY);
        if (planVersion != null && planVersion.equals(config.get(DefinitionVersion.CONFIG_KEY)) == false) {
            throw new ElasticsearchStatusException(
                "external dataset [{}] definition changed while the query was planned",
                RestStatus.BAD_REQUEST,
                name
            );
        }
        final List<ResourcePath> resources;
        try {
            resources = resourcePaths(resolved.dataset().resource());
        } catch (IllegalArgumentException e) {
            // PUT validation parses the resource for every file-type data source, so this is a definition this node's
            // readers cannot bind to; keep the one binding-failure message rather than leak the parser's.
            throw notRegistered(name);
        }
        for (ExternalSplit split : exec.splits()) {
            checkSplit(name, split, resources);
        }
        return exec.withSourcePathAndConfig(resolved.dataset().resource(), config);
    }

    /**
     * The paths a resource names, one per top-level comma segment, decomposed the way the listing does
     * ({@link GlobExpander#commaSegments}) so a comma inside a brace group stays part of its glob. An HTTP resource is
     * always one path: the listing never treats its commas as separators, so neither does this.
     */
    private static List<ResourcePath> resourcePaths(String resource) {
        StoragePath whole = StoragePath.of(resource);
        if (whole.scheme().equalsIgnoreCase("http") || whole.scheme().equalsIgnoreCase("https")) {
            return List.of(resourcePath(whole));
        }
        List<ResourcePath> paths = new ArrayList<>();
        for (String segment : GlobExpander.commaSegments(resource)) {
            paths.add(resourcePath(StoragePath.of(segment)));
        }
        return paths;
    }

    private static ResourcePath resourcePath(StoragePath path) {
        Predicate<String> globMatcher = path.isPattern() ? GlobExpander.compileGlobMatcher(path.globPart()) : null;
        return new ResourcePath(path, globMatcher);
    }

    /**
     * The refusal for a source this node cannot bind to a registered dataset: no name, unknown name, or a name whose
     * data source is gone. One message for all three.
     */
    public static ElasticsearchStatusException notRegistered(@Nullable String datasetName) {
        return new ElasticsearchStatusException(
            "external dataset [{}] is not registered on this node",
            RestStatus.BAD_REQUEST,
            datasetName == null ? "" : datasetName
        );
    }

    /**
     * Every leaf of {@code split} must lie within one of the resource's paths. {@code ExternalSplit} itself exposes no
     * path, so only the file-backed shapes are accepted: a {@link FileSplit}, or a {@link CoalescedSplit} whose children
     * all pass. See {@link #isWithin} for what "within" admits.
     */
    private static void checkSplit(String datasetName, ExternalSplit split, List<ResourcePath> resources) {
        if (split instanceof CoalescedSplit coalesced) {
            for (ExternalSplit child : coalesced.children()) {
                checkSplit(datasetName, child, resources);
            }
            return;
        }
        if (split instanceof FileSplit file && resources.stream().anyMatch(resource -> isWithin(file.path(), resource))) {
            return;
        }
        throw new ElasticsearchStatusException(
            "external dataset [{}] does not cover split [{}]",
            RestStatus.BAD_REQUEST,
            datasetName,
            split instanceof FileSplit file ? file.path() : split.getClass().getSimpleName()
        );
    }

    /**
     * Whether {@code path} is one the resource could have matched, decided lexically. The store must be the same:
     * scheme, host, port and user-info, the last because Azure's Hadoop form carries the container there
     * ({@code wasbs://container@account.../blob}), so two containers on one account differ only in it. Within the
     * store: a resource naming one object admits exactly that object; a directory admits everything under it; a glob
     * admits only paths matched by the same glob syntax as listing. A {@code ..} segment is refused outright: on a
     * {@code file://} resource it would step out of the directory while still starting with it.
     */
    static boolean isWithin(StoragePath path, StoragePath resource) {
        Predicate<String> globMatcher = resource.isPattern() ? GlobExpander.compileGlobMatcher(resource.globPart()) : null;
        return isWithin(path, resource, globMatcher);
    }

    private static boolean isWithin(StoragePath path, ResourcePath resource) {
        return isWithin(path, resource.path(), resource.globMatcher());
    }

    private static boolean isWithin(StoragePath path, StoragePath resource, @Nullable Predicate<String> globMatcher) {
        if (path.scheme().equalsIgnoreCase(resource.scheme()) == false
            || Objects.equals(lower(path.host()), lower(resource.host())) == false
            || path.port() != resource.port()
            || Objects.equals(path.userInfo(), resource.userInfo()) == false
            || hasParentSegment(path.path())) {
            return false;
        }
        if (resource.isPattern()) {
            StoragePath prefix = resource.patternPrefix();
            if (path.path().startsWith(prefix.path()) == false) {
                return false;
            }
            return globMatcher != null && globMatcher.test(path.path().substring(prefix.path().length()));
        }
        if (resource.scheme().equalsIgnoreCase("http") || resource.scheme().equalsIgnoreCase("https")) {
            return path.toString().equals(resource.toString());
        }
        String resourcePath = resource.path();
        return resourcePath.endsWith("/") ? path.path().startsWith(resourcePath) : path.path().equals(resourcePath);
    }

    private static boolean hasParentSegment(String path) {
        for (String segment : path.split("/")) {
            if (segment.equals("..")) {
                return true;
            }
        }
        return false;
    }

    private static String lower(@Nullable String host) {
        return host == null ? null : host.toLowerCase(Locale.ROOT);
    }
}
