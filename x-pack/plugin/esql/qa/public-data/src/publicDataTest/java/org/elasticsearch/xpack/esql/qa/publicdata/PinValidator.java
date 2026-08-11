/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.qa.publicdata;

import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinInfo;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PinStrategy;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.PublicDataProvider;
import org.elasticsearch.xpack.esql.qa.publicdata.catalog.SourceVariant;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Re-checks a {@link SourceVariant}'s {@link PinInfo} against the live object with a metadata-only HTTP
 * {@code HEAD} -- never a body fetch -- immediately before the suite queries it (plan section 3). A
 * mismatch means the upstream publisher's "immutable" object has actually changed since the pin was
 * captured, which would otherwise silently invalidate the checked-in expected results; this fails loudly
 * instead.
 */
public final class PinValidator {

    private static final Logger logger = LogManager.getLogger(PinValidator.class);
    // randomizedtesting's ThreadLeakControl reports any thread still alive at SUITE teardown as "leaked",
    // daemon or not -- daemon-ness only affects whether a thread blocks JVM exit, it is not itself a
    // leak-check exemption. HttpClient#close() (JDK 21+) does not shut down a custom executor passed via
    // Builder.executor(...), so this client is a suite-lifetime singleton (a handful of HEAD checks don't
    // warrant a client per test) paired with its own ExecutorService reference, both torn down explicitly
    // by #shutdown() from an @AfterClass in the suite -- see PublicDataSourcesIT.
    private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool(daemonThreadFactory());
    // NORMAL (not the default NEVER) so a HEAD survives a CDN-backed host that 302s to a signed, per-request
    // URL (e.g. Hugging Face's resolve endpoint) -- the query's own HttpStorageProvider follows redirects by
    // default too (HttpConfiguration#followRedirects), so this matches production behavior rather than
    // special-casing one host.
    private static final HttpClient CLIENT = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(20))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .executor(EXECUTOR)
        .build();

    private static ThreadFactory daemonThreadFactory() {
        ThreadFactory delegate = Executors.defaultThreadFactory();
        return runnable -> {
            Thread thread = delegate.newThread(runnable);
            thread.setDaemon(true);
            thread.setName("public-data-pin-check-" + thread.getName());
            return thread;
        };
    }

    /** Releases {@link #CLIENT}'s underlying threads; call once from the suite's {@code @AfterClass}. */
    public static void shutdown() throws InterruptedException {
        CLIENT.close();
        EXECUTOR.shutdownNow();
        EXECUTOR.awaitTermination(10, TimeUnit.SECONDS);
    }

    /** {@code s3://bucket/key} -&gt; virtual-hosted-style HTTPS, so a plain HEAD works with no AWS SDK/signing. */
    private static final Pattern S3_URI = Pattern.compile("^s3://([^/]+)/(.+)$");

    private PinValidator() {}

    /**
     * Issues a {@code HEAD} against {@code variant.pinCheckUri()}. For the default
     * {@link PinStrategy#ETAG}, asserts the live {@code ETag} and {@code Content-Length} match
     * {@code variant.pin()}, throwing {@link AssertionError} on a mismatch. For
     * {@link PinStrategy#CONTENT_SIGNATURE}, only confirms the object is reachable (status 200) -- ETag/
     * size drift is expected for those variants (see {@link PinStrategy#CONTENT_SIGNATURE}'s Javadoc) and
     * is logged, not asserted; {@code variant.pin().contentSignature()} is documentation-only and is
     * re-established by hand, not by this method, since doing so live would require the body fetch this
     * method never performs. Throws {@link IOException}/{@link InterruptedException} on a transport
     * failure (e.g. offline dev environment) -- callers should let a transport failure surface as a suite
     * failure rather than silently skip, since this suite is only ever invoked manually against real
     * remote endpoints.
     */
    public static void verify(SourceVariant variant) throws IOException, InterruptedException {
        PinInfo pin = variant.pin();
        URI headUri = toHttpsHeadUri(variant);
        HttpRequest request = HttpRequest.newBuilder(headUri).method("HEAD", HttpRequest.BodyPublishers.noBody()).build();
        HttpResponse<Void> response = CLIENT.send(request, HttpResponse.BodyHandlers.discarding());
        if (response.statusCode() != 200) {
            throw new AssertionError(
                "Pin check HEAD " + headUri + " for variant [" + variant.id() + "] returned status [" + response.statusCode() + "]"
            );
        }
        String liveEtag = response.headers().firstValue("etag").orElse(null);
        long liveSize = response.headers().firstValueAsLong("content-length").orElse(-1L);
        if (pin.strategy() == PinStrategy.CONTENT_SIGNATURE) {
            logger.info(
                "Pin check OK (content-signature strategy, ETag/size drift not asserted) for variant [{}]: {} "
                    + "(pinned etag={}, live etag={}, pinned size={}, live size={}, content signature=[{}] captured on [{}])",
                variant.id(),
                headUri,
                pin.etag(),
                liveEtag,
                pin.sizeBytes(),
                liveSize,
                pin.contentSignature(),
                pin.capturedAt()
            );
            return;
        }
        if (liveEtag != null && liveEtag.equals(pin.etag()) == false) {
            throw new AssertionError(
                "Variant ["
                    + variant.id()
                    + "] ETag drifted: catalog pins ["
                    + pin.etag()
                    + "] but live HEAD of ["
                    + headUri
                    + "] returned ["
                    + liveEtag
                    + "]; the upstream object changed since this pin was captured on ["
                    + pin.capturedAt()
                    + "], so checked-in expected results may no longer be valid"
            );
        }
        if (liveSize >= 0 && liveSize != pin.sizeBytes()) {
            throw new AssertionError(
                "Variant ["
                    + variant.id()
                    + "] size drifted: catalog pins ["
                    + pin.sizeBytes()
                    + "] bytes but live HEAD of ["
                    + headUri
                    + "] returned ["
                    + liveSize
                    + "] bytes"
            );
        }
        logger.info("Pin check OK for variant [{}]: {} (etag={}, size={})", variant.id(), headUri, liveEtag, liveSize);
    }

    /** Converts {@code variant.pinCheckUri()} to a plain-HTTPS URI a {@code HEAD} can be issued against. */
    private static URI toHttpsHeadUri(SourceVariant variant) {
        String uri = variant.pinCheckUri();
        if (variant.provider() == PublicDataProvider.S3) {
            Matcher m = S3_URI.matcher(uri);
            if (m.matches() == false) {
                throw new IllegalArgumentException("Variant [" + variant.id() + "] pinCheckUri [" + uri + "] is not a valid s3:// URI");
            }
            String bucket = m.group(1);
            String key = m.group(2);
            String region = variant.region() == null ? "us-east-1" : variant.region();
            // Virtual-hosted-style: https://<bucket>.s3.<region>.amazonaws.com/<key>. us-east-1 also accepts the
            // regionless https://<bucket>.s3.amazonaws.com/<key> form, but the explicit-region form works for
            // every region and needs no per-region special-casing.
            return URI.create(String.format(Locale.ROOT, "https://%s.s3.%s.amazonaws.com/%s", bucket, region, key));
        }
        // HTTPS (and, once populated, GCS/AZURE) pinCheckUri values are already directly HEAD-able HTTPS URIs.
        return URI.create(uri);
    }
}
