/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * =============================================================================
 *
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.identity.spi.AwsCredentialsIdentity;
import software.amazon.awssdk.identity.spi.ResolveIdentityRequest;
import software.amazon.awssdk.regions.util.HttpResourcesUtils;
import software.amazon.awssdk.regions.util.ResourcesEndpointProvider;
import software.amazon.awssdk.regions.util.ResourcesEndpointRetryPolicy;
import software.amazon.awssdk.utils.DateUtils;
import software.amazon.awssdk.utils.cache.CachedSupplier;
import software.amazon.awssdk.utils.cache.RefreshResult;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.watcher.WatcherHandle;
import org.elasticsearch.xcontent.XContentType;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * Container-credentials provider for EKS Pod Identity that reads the auth token from the
 * entitled symlink under {@code ${ES_PATH_CONF}} rather than from
 * {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE}.
 *
 * <p>Adapted from the AWS SDK for Java v2 {@code ContainerCredentialsProvider}; the ECS/EKS host
 * constants must be re-checked on AWS SDK bumps.
 *
 * <p>The AWS SDK's {@code ContainerCredentialsProvider} is {@code final} and only resolves the
 * token path from {@link SdkSystemSetting} (env var / JVM system property). Writing the system
 * property would redirect every AWS SDK client in the process — including {@code repository-s3} —
 * so this provider owns the HTTP exchange itself: it reads the entitled token file, calls the
 * endpoint named by {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}, and caches the result.
 *
 * <p>Active only when both {@code AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE} and
 * {@code AWS_CONTAINER_CREDENTIALS_FULL_URI} are set (the Pod Identity shape) and a readable file
 * exists at the entitled symlink. When the env vars are set but the symlink is missing or
 * unreadable, {@link #isMisconfigured()} is true and {@link #misconfigurationMessage()} names the
 * file the operator must create — callers must not fall through to the stock SDK provider (which
 * would hit the entitlement-blocked Kubernetes path). If an earlier chain link such as IRSA is
 * already active, the container slot may be skipped instead of failing the whole path.
 */
public class EsqlContainerCredentialsProvider implements AwsCredentialsProvider, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(EsqlContainerCredentialsProvider.class);

    /** Operator-managed symlink location, relative to {@code ${ES_PATH_CONF}}. */
    public static final String POD_IDENTITY_TOKEN_FILE_LOCATION = "esql-datasource-s3/eks-pod-identity-token";

    private static final String PROVIDER_NAME = "EsqlContainerCredentialsProvider";

    /** Matches trailing {@code +0000} / {@code +00:00} so Expiration parses as UTC. */
    private static final Pattern TRAILING_ZERO_OFFSET_TIME = Pattern.compile("\\+00(?:00|:00)$");

    /** ECS task-metadata IPv4 host allowed for non-HTTPS container-credentials URIs (AWS SDK parity). */
    private static final String ECS_CONTAINER_HOST = "169.254.170.2";
    /** EKS Pod Identity Agent IPv4 host allowed for non-HTTPS container-credentials URIs. */
    private static final String EKS_CONTAINER_HOST_IPV4 = "169.254.170.23";
    /** EKS Pod Identity Agent IPv6 host allowed for non-HTTPS container-credentials URIs. */
    private static final String EKS_CONTAINER_HOST_IPV6 = "fd00:ec2::23";

    private static final int MAX_CREDENTIAL_FETCH_RETRIES = 5;

    private final Path tokenFileLocation;
    /**
     * Raw {@code AWS_CONTAINER_CREDENTIALS_FULL_URI} value. Parsed and host-validated only on
     * first credential fetch ({@link EntitledTokenEndpointProvider#endpoint()}) so a malformed URI
     * does not break anonymous storage-provider construction on a node that only uses
     * {@code repository-s3} Pod Identity.
     */
    private final String credentialsUri;
    /**
     * Non-null when the Pod Identity env vars are set but the entitled symlink is missing or
     * unreadable. Callers must not fall through to the stock SDK provider; see {@link #isMisconfigured()}.
     */
    private final String misconfigurationMessage;
    /**
     * Guards read-modify-write of {@link #credentialsCache} / {@link #closed} between
     * {@link #close()} and the token-file watcher, so a post-close cache cannot be installed.
     */
    private final Object cacheLock = new Object();
    private volatile CachedSupplier<AwsCredentials> credentialsCache;
    private WatcherHandle<FileWatcher> watcherHandle;
    /** Set by {@link #close()}; watcher callbacks must not install a new cache after close. */
    private volatile boolean closed;
    /**
     * Lazily validated once from {@link #credentialsUri}. Host allow-list checks can involve DNS;
     * the URI is immutable for the provider lifetime so re-validating on every refresh is wasteful.
     */
    private volatile URI validatedCredentialsEndpoint;

    public EsqlContainerCredentialsProvider(Environment environment, ResourceWatcherService resourceWatcherService) {
        this(environment, resourceWatcherService, System::getenv);
    }

    /**
     * Test seam: env-var lookups are routed through {@code envLookup} so unit tests can inject a
     * stub map without manipulating real {@code System.getenv} state.
     */
    @SuppressWarnings("this-escape")
    EsqlContainerCredentialsProvider(
        Environment environment,
        ResourceWatcherService resourceWatcherService,
        Function<String, String> envLookup
    ) {
        final String tokenFileEnv = envLookup.apply(SdkSystemSetting.AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE.environmentVariable());
        final String credentialsUriEnv = envLookup.apply(SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable());
        if (Strings.hasText(tokenFileEnv) == false || Strings.hasText(credentialsUriEnv) == false) {
            this.tokenFileLocation = null;
            this.credentialsUri = null;
            this.misconfigurationMessage = null;
            return;
        }
        if (environment == null) {
            LOGGER.warn(
                "Cannot configure EKS Pod Identity: node environment is unavailable "
                    + "(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE=[{}] will not be used for ESQL S3 reads)",
                tokenFileEnv
            );
            this.tokenFileLocation = null;
            this.credentialsUri = null;
            this.misconfigurationMessage = null;
            return;
        }

        final Path entitledPath = environment.configDir().resolve(POD_IDENTITY_TOKEN_FILE_LOCATION);
        this.tokenFileLocation = entitledPath;
        this.credentialsUri = credentialsUriEnv;

        if (Files.exists(entitledPath) == false) {
            this.misconfigurationMessage = Strings.format(
                "Cannot use EKS Pod Identity: AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE is defined as [%s] but Elasticsearch requires a "
                    + "symlink to this token file at location [%s] and there is nothing at that location. Create it to point at the "
                    + "projected service-account token, then restart the node.",
                tokenFileEnv,
                entitledPath
            );
            LOGGER.info(misconfigurationMessage);
            return;
        }
        if (Files.isReadable(entitledPath) == false) {
            // Soft misconfiguration (same as missing): managed_identity fails loudly via
            // isMisconfigured(), but anonymous / other auth modes that only construct the provider
            // at plugin init must not blow up — a node may only have the repository-s3 symlink.
            this.misconfigurationMessage = Strings.format(
                "Cannot use EKS Pod Identity: AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE is defined as [%s] but Elasticsearch requires a "
                    + "symlink to this token file at location [%s] and this location is not readable. Fix permissions, then restart "
                    + "the node.",
                tokenFileEnv,
                entitledPath
            );
            LOGGER.info(misconfigurationMessage);
            return;
        }

        this.misconfigurationMessage = null;
        this.credentialsCache = CachedSupplier.builder(this::refreshCredentials).cachedValueName(toString()).build();
        setupFileWatcherToRefreshCredentials(entitledPath, resourceWatcherService);
    }

    /**
     * Mirrors the AWS SDK {@code ContainerCredentialsProvider} URI gate: HTTPS is always allowed;
     * plain HTTP is allowed only when the host is loopback or the well-known ECS / EKS Pod Identity
     * metadata addresses. Without this check a compromised {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}
     * could exfiltrate the Authorization token to an arbitrary host.
     */
    static URI validateCredentialsEndpoint(URI uri) {
        if ("https".equalsIgnoreCase(uri.getScheme())) {
            return uri;
        }
        String host = uri.getHost();
        if (host == null) {
            throw SdkClientException.create(
                "The full URI ("
                    + uri
                    + ") contained within environment variable "
                    + SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable()
                    + " has no host."
            );
        }
        if (isAllowedHttpHost(host) == false) {
            throw SdkClientException.create(
                Strings.format(
                    "The full URI (%s) contained within environment variable %s has an invalid host. "
                        + "Host should resolve to a loopback address or have the full URI be HTTPS.",
                    uri,
                    SdkSystemSetting.AWS_CONTAINER_CREDENTIALS_FULL_URI.environmentVariable()
                )
            );
        }
        return uri;
    }

    private static boolean isAllowedHttpHost(String host) {
        if (ECS_CONTAINER_HOST.equals(host)
            || EKS_CONTAINER_HOST_IPV4.equals(host)
            || EKS_CONTAINER_HOST_IPV6.equals(host)
            || ("[" + EKS_CONTAINER_HOST_IPV6 + "]").equals(host)) {
            return true;
        }
        try {
            InetAddress[] addresses = InetAddress.getAllByName(host);
            for (InetAddress address : addresses) {
                if (address.isLoopbackAddress() == false) {
                    return false;
                }
            }
            return true;
        } catch (UnknownHostException e) {
            throw SdkClientException.create(Strings.format("host (%s) could not be resolved to an IP address.", host), e);
        }
    }

    private void setupFileWatcherToRefreshCredentials(Path tokenSymlink, ResourceWatcherService resourceWatcherService) {
        if (resourceWatcherService == null) {
            return;
        }
        FileWatcher watcher = new FileWatcher(tokenSymlink);
        watcher.addListener(new FileChangesListener() {
            @Override
            public void onFileCreated(Path file) {
                onFileChanged(file);
            }

            @Override
            public void onFileChanged(Path file) {
                if (file.equals(tokenSymlink) == false) {
                    return;
                }
                LOGGER.debug("EKS Pod Identity token file [{}] changed, refreshing credentials", file);
                // Bust the cache so the next resolve re-reads the token and re-exchanges it.
                CachedSupplier<AwsCredentials> previous;
                synchronized (cacheLock) {
                    if (closed) {
                        return;
                    }
                    previous = credentialsCache;
                    credentialsCache = CachedSupplier.builder(EsqlContainerCredentialsProvider.this::refreshCredentials)
                        .cachedValueName(EsqlContainerCredentialsProvider.this.toString())
                        .build();
                }
                if (previous != null) {
                    previous.close();
                }
            }
        });
        try {
            watcherHandle = resourceWatcherService.add(watcher, ResourceWatcherService.Frequency.LOW);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching EKS Pod Identity token file [{}]", e, tokenSymlink);
        }
    }

    private RefreshResult<AwsCredentials> refreshCredentials() {
        try {
            // HttpResourcesUtils / ResourcesEndpointProvider are @SdkProtectedApi; check on AWS SDK bumps.
            String body = HttpResourcesUtils.instance().readResource(new EntitledTokenEndpointProvider());
            ParsedCredentials parsed = parseCredentialsResponse(body);
            Instant now = Instant.now();
            Instant expiration = parsed.expiration();
            return RefreshResult.builder(parsed.credentials())
                .staleTime(staleTime(now, expiration))
                .prefetchTime(prefetchTime(now, expiration))
                .build();
        } catch (IOException e) {
            throw SdkClientException.builder().message("Failed to load EKS Pod Identity credentials.").cause(e).build();
        }
    }

    /**
     * Stale schedule matching the AWS SDK {@code ContainerCredentialsProvider}
     * ({@code expiration - 1 minute}), with a floor for short-lived tokens. When TTL is under one
     * minute the stock formula puts {@code staleTime} in the past, so {@link CachedSupplier} treats
     * the value as immediately stale and blocks on an HTTP refresh on every
     * {@link #resolveCredentials()} call. Floor to half the remaining lifetime in that case.
     */
    static Instant staleTime(Instant now, Instant expiration) {
        if (expiration == null) {
            return null;
        }
        Instant candidate = expiration.minus(1, ChronoUnit.MINUTES);
        if (candidate.isAfter(now)) {
            return candidate;
        }
        long remainingMillis = ChronoUnit.MILLIS.between(now, expiration);
        if (remainingMillis <= 0) {
            return now;
        }
        return now.plusMillis(remainingMillis / 2);
    }

    /**
     * Prefetch schedule matching the AWS SDK {@code ContainerCredentialsProvider}, with a floor for
     * short-lived tokens. The stock formula {@code min(now+1h, expiration-15min)} is ≤ {@code now}
     * when credential lifetime is ≤ 15 minutes, which would make {@link CachedSupplier} schedule an
     * immediate tight refresh loop. In that case prefetch at half the remaining lifetime instead.
     * (Production EKS Pod Identity credentials last six hours by default, so they use the stock
     * formula; the floor matters for short-lived fixtures and any other short TTL.)
     */
    static Instant prefetchTime(Instant now, Instant expiration) {
        Instant oneHourFromNow = now.plus(1, ChronoUnit.HOURS);
        if (expiration == null) {
            return oneHourFromNow;
        }
        Instant fifteenMinutesBeforeExpiration = expiration.minus(15, ChronoUnit.MINUTES);
        Instant candidate = min(oneHourFromNow, fifteenMinutesBeforeExpiration);
        if (candidate.isAfter(now)) {
            return candidate;
        }
        long remainingMillis = ChronoUnit.MILLIS.between(now, expiration);
        if (remainingMillis <= 0) {
            return now;
        }
        return now.plusMillis(remainingMillis / 2);
    }

    private static Instant min(Instant a, Instant b) {
        return a.isBefore(b) ? a : b;
    }

    private static ParsedCredentials parseCredentialsResponse(String body) {
        Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), body, false);
        String accessKeyId = requireString(map, "AccessKeyId");
        String secretAccessKey = requireString(map, "SecretAccessKey");
        String token = stringOrNull(map, "Token");
        String expirationRaw = stringOrNull(map, "Expiration");
        Instant expiration = null;
        if (expirationRaw != null) {
            expiration = DateUtils.parseIso8601Date(TRAILING_ZERO_OFFSET_TIME.matcher(expirationRaw).replaceAll("Z"));
        }
        AwsCredentials credentials;
        if (Strings.hasText(token)) {
            credentials = AwsSessionCredentials.builder()
                .accessKeyId(accessKeyId)
                .secretAccessKey(secretAccessKey)
                .sessionToken(token)
                .providerName(PROVIDER_NAME)
                .build();
        } else {
            credentials = AwsBasicCredentials.builder()
                .accessKeyId(accessKeyId)
                .secretAccessKey(secretAccessKey)
                .providerName(PROVIDER_NAME)
                .build();
        }
        return new ParsedCredentials(credentials, expiration);
    }

    private static String requireString(Map<String, Object> map, String key) {
        String value = stringOrNull(map, key);
        if (Strings.hasText(value) == false) {
            throw SdkClientException.create("Failed to load " + key + " from container credentials response.");
        }
        return value;
    }

    private static String stringOrNull(Map<String, Object> map, String key) {
        Object value = map.get(key);
        return value == null ? null : value.toString();
    }

    /**
     * {@code true} when the provider was successfully wired (Pod Identity env present and entitled
     * token readable) and has not been {@link #close() closed}. Callers gate inclusion in the
     * credentials chain on this signal.
     */
    public boolean isActive() {
        return closed == false && credentialsCache != null;
    }

    /**
     * {@code true} when the Pod Identity env vars are set but the entitled symlink is missing or
     * was unreadable at construction. Callers must not fall through to the stock
     * {@code ContainerCredentialsProvider}; if no earlier provider is in the chain they should fail
     * with {@link #misconfigurationMessage()}, otherwise they may skip this slot.
     */
    public boolean isMisconfigured() {
        return misconfigurationMessage != null && credentialsCache == null;
    }

    /** Message naming the entitled symlink the operator must create; only valid when {@link #isMisconfigured()}. */
    public String misconfigurationMessage() {
        return misconfigurationMessage;
    }

    @Override
    public void close() {
        WatcherHandle<FileWatcher> handle;
        CachedSupplier<AwsCredentials> cache;
        synchronized (cacheLock) {
            closed = true;
            handle = watcherHandle;
            watcherHandle = null;
            cache = credentialsCache;
            credentialsCache = null;
        }
        if (handle != null) {
            handle.stop();
        }
        if (cache != null) {
            cache.close();
        }
    }

    @Override
    public AwsCredentials resolveCredentials() {
        CachedSupplier<AwsCredentials> cache = credentialsCache;
        Objects.requireNonNull(cache, "credentialsCache is not set");
        return cache.get();
    }

    @Override
    public Class<AwsCredentialsIdentity> identityType() {
        return AwsCredentialsIdentity.class;
    }

    @Override
    public CompletableFuture<AwsCredentialsIdentity> resolveIdentity(ResolveIdentityRequest request) {
        return CompletableFuture.completedFuture(resolveCredentials());
    }

    @Override
    public String toString() {
        return PROVIDER_NAME + "[" + tokenFileLocation + " -> " + credentialsUri + "]";
    }

    /**
     * {@link ResourcesEndpointProvider} that points at {@code AWS_CONTAINER_CREDENTIALS_FULL_URI}
     * and supplies the entitled token as the {@code Authorization} header — the same exchange the
     * SDK's container provider performs, without reading the token path from process-wide state.
     */
    private final class EntitledTokenEndpointProvider implements ResourcesEndpointProvider {
        @Override
        public URI endpoint() {
            URI cached = validatedCredentialsEndpoint;
            if (cached != null) {
                return cached;
            }
            URI validated = validateCredentialsEndpoint(URI.create(credentialsUri));
            validatedCredentialsEndpoint = validated;
            return validated;
        }

        @Override
        public Map<String, String> headers() {
            try {
                String token = Files.readString(tokenFileLocation, StandardCharsets.UTF_8);
                return Map.of("Authorization", token);
            } catch (IOException e) {
                throw SdkClientException.create("Failed to read " + tokenFileLocation.toAbsolutePath() + ".", e);
            }
        }

        @Override
        public ResourcesEndpointRetryPolicy retryPolicy() {
            // Parity with the SDK's ContainerCredentialsRetryPolicy: retry transient 5xx / IO failures.
            return (retriesAttempted, retryParameters) -> {
                if (retriesAttempted >= MAX_CREDENTIAL_FETCH_RETRIES) {
                    return false;
                }
                Integer statusCode = retryParameters.getStatusCode();
                if (statusCode != null && statusCode >= 500 && statusCode < 600) {
                    return true;
                }
                Exception exception = retryParameters.getException();
                return exception instanceof IOException;
            };
        }
    }

    private record ParsedCredentials(AwsCredentials credentials, Instant expiration) {}
}
