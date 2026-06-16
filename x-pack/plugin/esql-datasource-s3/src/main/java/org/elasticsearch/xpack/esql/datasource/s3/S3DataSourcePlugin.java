/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.core.SdkSystemSetting;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourcePlugin;
import org.elasticsearch.xpack.esql.datasources.spi.DataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.FileDataSourceValidator;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProviderFactory;

import java.io.IOException;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Data source plugin providing S3 storage support for ESQL.
 * Supports s3://, s3a://, and s3n:// URI schemes.
 */
public class S3DataSourcePlugin extends Plugin implements DataSourcePlugin {

    private static final Logger LOGGER = LogManager.getLogger(S3DataSourcePlugin.class);

    /**
     * Operator-managed symlink for EKS Pod Identity, relative to {@code ${ES_PATH_CONF}}.
     * The plugin's {@code entitlement-policy.yaml} grants read access to this fixed location.
     */
    public static final String POD_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/eks-pod-identity-token";

    /**
     * Node-wide IRSA web-identity provider singleton. Static (rather than instance) on purpose:
     * {@link org.elasticsearch.xpack.esql.plugin.EsqlPlugin}'s {@code DataSourcePlugin} SPI
     * creates its own {@link S3DataSourcePlugin} instance via reflection (see
     * {@code PluginsService#createExtension}), so the instance whose {@link #storageProviders}
     * runs is NOT the same one whose {@link #createComponents} received a
     * {@link PluginServices}. Hoisting the singleton to a static field is what lets both
     * instances share a single IRSA provider, file watcher, and STS client.
     *
     * <p>Not safe across multiple {@link org.elasticsearch.node.Node} instances in the same JVM:
     * the file watcher is registered against a specific node's {@code ResourceWatcherService},
     * and {@link #close()} is first-wins, so a second node's shutdown would close the singleton
     * out from under the first. {@code ESRestTestCase}-style tests fork a JVM per cluster and
     * are unaffected.
     */
    private static final AtomicReference<CustomWebIdentityTokenCredentialsProvider> IRSA_PROVIDER = new AtomicReference<>();

    /**
     * Tracks the entitled path we set on the {@code aws.containerAuthorizationTokenFile} sysprop
     * inside {@link #maybeOverrideContainerAuthTokenFile}, so {@link #close()} can clear the
     * sysprop only when it still matches what we set (i.e. nothing else has clobbered it).
     */
    private static final AtomicReference<String> POD_IDENTITY_SYSPROP_SET_TO = new AtomicReference<>();

    @Override
    public Set<String> supportedSchemes() {
        return Set.of("s3", "s3a", "s3n");
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        // Build the IRSA web-identity provider as a node-level singleton so the file watcher and
        // STS client live for the lifetime of the node rather than being rebuilt per query.
        // The provider self-disables (isActive() == false) when AWS_WEB_IDENTITY_TOKEN_FILE is
        // unset, so non-EKS deployments incur no cost beyond construction.
        if (IRSA_PROVIDER.get() == null) {
            CustomWebIdentityTokenCredentialsProvider provider = new CustomWebIdentityTokenCredentialsProvider(
                services.environment(),
                Clock.systemUTC(),
                services.resourceWatcherService()
            );
            if (IRSA_PROVIDER.compareAndSet(null, provider) == false) {
                // Another instance won the race; close ours so we don't leak the STS client and
                // file watcher.
                IOUtils.closeWhileHandlingException(provider::close);
            }
        }

        // EKS Pod Identity: AWS SDK v2's ContainerCredentialsProvider is final and reads
        // AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE directly. The Kubernetes-injected path
        // (/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token) is
        // outside the entitlement-grantable area, so we redirect the SDK at the entitled symlink
        // location via the JVM system property override (sysprop has higher precedence than env
        // var in SdkSystemSetting). This is JVM-global; document the implication for any other
        // AWS SDK ContainerCredentialsProvider users in the same JVM.
        maybeOverrideContainerAuthTokenFile(services);
        return List.of();
    }

    @SuppressForbidden(reason = "JVM system property is the only override knob for the final AWS SDK ContainerCredentialsProvider")
    private static void maybeOverrideContainerAuthTokenFile(PluginServices services) {
        String envValue = System.getenv(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable());
        if (Strings.hasText(envValue) == false) {
            return;
        }
        String entitledPath = services.environment().configDir().resolve(POD_IDENTITY_TOKEN_FILE_LOCATION).toString();
        String existing = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        if (existing != null && existing.equals(entitledPath) == false) {
            // Something else already pinned the SDK to a different token-file path. We cannot override it
            // without breaking that component, and leaving it in place would silently point our chain's
            // ContainerCredentialsProvider at the wrong token file. Fail fast rather than mis-authenticate.
            throw new IllegalStateException(
                Strings.format(
                    "Cannot configure EKS Pod Identity for the S3 data source: the JVM system property [%s] is already set to [%s], "
                        + "but it must point at the entitled token symlink [%s]. Remove the conflicting "
                        + "-D%s setting (or align it with the entitled location) before starting the node.",
                    SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property(),
                    existing,
                    entitledPath,
                    SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property()
                )
            );
        }
        System.setProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property(), entitledPath);
        POD_IDENTITY_SYSPROP_SET_TO.set(entitledPath);
        LOGGER.debug("Redirected AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE [{}] to [{}] for EKS Pod Identity", envValue, entitledPath);
    }

    @Override
    public Map<String, StorageProviderFactory> storageProviders(Settings settings, ExecutorService executor) {
        // Read the singleton inside the factory lambdas (not at method entry) so that a future
        // call ordering where storageProviders runs before createComponents still picks up the
        // singleton on the next factory invocation, instead of permanently capturing a null
        // reference. See IRSA_PROVIDER javadoc.
        StorageProviderFactory s3Factory = StorageProviderFactory.of(
            () -> new S3StorageProvider(null, IRSA_PROVIDER.get()),
            S3Configuration::fromQueryConfig,
            cfg -> new S3StorageProvider(cfg, IRSA_PROVIDER.get())
        );
        return Map.of("s3", s3Factory, "s3a", s3Factory, "s3n", s3Factory);
    }

    @Override
    public Map<String, DataSourceValidator> datasourceValidators(Settings settings) {
        DataSourceValidator v = new FileDataSourceValidator("s3", S3Configuration::fromMap, supportedSchemes());
        return Map.of(v.type(), v);
    }

    @Override
    public void close() throws IOException {
        // Static singleton: clear and close exactly once even though close() may run on either
        // the lifecycle or SPI-discovered instance (or both, if ES happens to call it twice).
        CustomWebIdentityTokenCredentialsProvider provider = IRSA_PROVIDER.getAndSet(null);
        // Use IOUtils.close (which propagates IOException) rather than the createComponents path's
        // closeWhileHandlingException: a race loser at startup should not break node startup,
        // but a real I/O failure tearing down the STS client at shutdown should surface.
        if (provider != null) {
            IOUtils.close(provider::close);
        }
        clearPodIdentitySysprop();
    }

    /**
     * Symmetric counterpart to {@link #maybeOverrideContainerAuthTokenFile}. Clears the JVM-global
     * {@code aws.containerAuthorizationTokenFile} sysprop, but only if its current value still
     * equals the entitled path we set. If something else clobbered it after we set it, we leave
     * it alone.
     */
    @SuppressForbidden(reason = "symmetric cleanup of the JVM-global sysprop set by maybeOverrideContainerAuthTokenFile")
    private static void clearPodIdentitySysprop() {
        String setTo = POD_IDENTITY_SYSPROP_SET_TO.getAndSet(null);
        if (setTo == null) {
            return;
        }
        String current = System.getProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        if (setTo.equals(current)) {
            System.clearProperty(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.property());
        }
    }
}
