/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.utils.SourceDestValidations;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportPutTransformAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportPutTransformAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final TransformAuditor auditor;
    private final SourceDestValidator sourceDestValidator;
    private final IngestService ingestService;

    @Inject
    public TransportPutTransformAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        this(
            PutTransformAction.NAME,
            settings,
            transportService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            clusterService,
            licenseState,
            transformServices,
            client,
            ingestService
        );
    }

    protected TransportPutTransformAction(
        String name,
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        super(
            name,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutTransformAction.Request::new,
            indexNameExpressionResolver
        );
        this.licenseState = licenseState;
        this.client = client;
        this.transformConfigManager = transformServices.getConfigManager();
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.auditor = transformServices.getAuditor();
        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            DiscoveryNode.isRemoteClusterClient(settings)
                ? new RemoteClusterLicenseChecker(client, XPackLicenseState::isTransformAllowedForOperationMode)
                : null,
            clusterService.getNodeName(),
            License.OperationMode.BASIC.description()
        );
        this.ingestService = ingestService;
    }

    static HasPrivilegesRequest buildPrivilegeCheck(
        TransformConfig config,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterState clusterState,
        String username
    ) {
        final String destIndex = config.getDestination().getIndex();
        final String[] concreteDest = indexNameExpressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination().getIndex()
        );
        List<String> srcPrivileges = new ArrayList<>(2);
        srcPrivileges.add("read");

        List<String> destPrivileges = new ArrayList<>(3);
        destPrivileges.add("read");
        destPrivileges.add("index");
        // If the destination index does not exist, we can assume that we may have to create it on start.
        // We should check that the creating user has the privileges to create the index.
        if (concreteDest.length == 0) {
            destPrivileges.add("create_index");
            // We need to read the source indices mapping to deduce the destination mapping
            srcPrivileges.add("view_index_metadata");
        }
        RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(destIndex)
            .privileges(destPrivileges)
            .build();

        RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(config.getSource().getIndex())
            .privileges(srcPrivileges)
            .build();

        HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
        privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        privRequest.username(username);
        privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
        privRequest.indexPrivileges(sourceIndexPrivileges, destIndexPrivileges);
        return privRequest;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener) {

        if (!licenseState.checkFeature(XPackLicenseState.Feature.TRANSFORM)) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.TRANSFORM));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        // set headers to run transform as calling user
        Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());

        TransformConfig config = request.getConfig().setHeaders(filteredHeaders).setCreateTime(Instant.now()).setVersion(Version.CURRENT);

        String transformId = config.getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(
                new ResourceAlreadyExistsException(TransformMessages.getMessage(TransformMessages.REST_PUT_TRANSFORM_EXISTS, transformId))
            );
            return;
        }

        sourceDestValidator.validate(
            clusterState,
            config.getSource().getIndex(),
            config.getDestination().getIndex(),
            request.isDeferValidation() ? SourceDestValidations.NON_DEFERABLE_VALIDATIONS : SourceDestValidations.ALL_VALIDATIONS,
            ActionListener.wrap(
                validationResponse -> {
                    // Early check to verify that the user can create the destination index and can read from the source
                    if (licenseState.isSecurityEnabled() && request.isDeferValidation() == false) {
                        final String username = securityContext.getUser().principal();
                        HasPrivilegesRequest privRequest = buildPrivilegeCheck(config, indexNameExpressionResolver, clusterState, username);
                        ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                            r -> handlePrivsResponse(username, request, r, listener),
                            listener::onFailure
                        );

                        client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
                    } else { // No security enabled, just create the transform
                        putTransform(request, listener);
                    }
                },
                listener::onFailure
            )
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void handlePrivsResponse(
        String username,
        Request request,
        HasPrivilegesResponse privilegesResponse,
        ActionListener<AcknowledgedResponse> listener
    ) {
        if (privilegesResponse.isCompleteMatch()) {
            putTransform(request, listener);
        } else {
            List<String> indices = privilegesResponse.getIndexPrivileges()
                .stream()
                .map(ResourcePrivileges::getResource)
                .collect(Collectors.toList());

            listener.onFailure(
                Exceptions.authorizationError(
                    "Cannot create transform [{}] because user {} lacks all the required permissions for indices: {}",
                    request.getConfig().getId(),
                    username,
                    indices
                )
            );
        }
    }

    private void putTransform(Request request, ActionListener<AcknowledgedResponse> listener) {

        final TransformConfig config = request.getConfig();
        // create the function for validation
        final Function function = FunctionFactory.create(config);

        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(putTransformConfigurationResult -> {
            logger.debug("[{}] created transform", config.getId());
            auditor.info(config.getId(), "Created transform.");
            listener.onResponse(new AcknowledgedResponse(true));
        }, listener::onFailure);

        // <2> Put our transform
        ActionListener<Boolean> validationListener = ActionListener.wrap(
            validationResult -> transformConfigManager.putTransformConfiguration(config, putTransformConfigurationListener),
            validationException -> {
                if (validationException instanceof ElasticsearchStatusException) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            TransformMessages.REST_PUT_TRANSFORM_FAILED_TO_VALIDATE_CONFIGURATION,
                            ((ElasticsearchStatusException) validationException).status(),
                            validationException
                        )
                    );
                } else {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            TransformMessages.REST_PUT_TRANSFORM_FAILED_TO_VALIDATE_CONFIGURATION,
                            RestStatus.INTERNAL_SERVER_ERROR,
                            validationException
                        )
                    );
                }
            }
        );

        function.validateConfig(ActionListener.wrap(r2 -> {
            if (request.isDeferValidation()) {
                validationListener.onResponse(true);
            } else {
                if (config.getDestination().getPipeline() != null) {
                    if (ingestService.getPipeline(config.getDestination().getPipeline()) == null) {
                        listener.onFailure(
                            new ElasticsearchStatusException(
                                TransformMessages.getMessage(TransformMessages.PIPELINE_MISSING, config.getDestination().getPipeline()),
                                RestStatus.BAD_REQUEST
                            )
                        );
                        return;
                    }
                }
                if (request.isDeferValidation()) {
                    validationListener.onResponse(true);
                } else {
                    function.validateQuery(client, config.getSource(), validationListener);
                }
            }
        }, listener::onFailure));
    }
}
