/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.s3;

import software.amazon.awssdk.services.s3.model.ArchiveStatus;
import software.amazon.awssdk.services.s3.model.IntelligentTieringAccessTier;
import software.amazon.awssdk.services.s3.model.InvalidObjectStateException;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalCredentialsExpiredException;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Renders why an S3 call failed, for appending to the message of the {@link java.io.IOException} that carries it up
 * to ES|QL.
 * <p>
 * It exists because naming only the operation and the path is not enough to act on: a refused connection, a wrong
 * access key, and a bucket that does not exist all produce the same "&lt;operation&gt; failed for &lt;path&gt;", and
 * the part that says which one it was survives only in a nested cause that most clients never render. Every site in
 * this plugin that turns an SDK failure into an {@code IOException} appends this, so the four operations
 * (HeadObject, range GET, existence probe, object read) report the condition the same way.
 * <p>
 * For an {@link S3Exception} that means the HTTP status plus the store's own error code ({@code AccessDenied},
 * {@code NoSuchBucket}) rather than the SDK's full {@code toString}, which also carries a request id, an attempt
 * count and the configured credentials provider's identity — noise here, and in the provider's case an object hash
 * that differs between runs.
 * <p>
 * It also classifies the failures whose remedy the status alone does not give: expired session credentials, a 403
 * whose message names the IAM action refused, credentials S3 did not accept, and an archived object.
 */
final class S3FailureDetail {

    // Real SDK chains here are 2-4 deep; this only stops a pathological one.
    private static final int MAX_CAUSE_DEPTH = 12;

    /**
     * AWS session-token failures. Matched by error code only — a bare HTTP 400/403 is not expiry
     * (Hadoop's HEAD trap, malformed ranges, {@code AuthorizationHeaderMalformed}).
     * {@code TokenRefreshRequired} is in this set because Standard already refuses the 400;
     * minting a new {@code managed_identity}/{@code federated_identity} signature is a separate
     * credential-refresh step, not a retry of the same signed GET.
     */
    private static final Set<String> CREDENTIALS_EXPIRED_CODES = Set.of("ExpiredToken", "InvalidToken", "TokenRefreshRequired");

    /** 403 codes with which S3 refuses the request's credentials themselves, rather than a permission they lack. */
    private static final Set<String> CREDENTIALS_REJECTED_CODES = Set.of("InvalidAccessKeyId", "SignatureDoesNotMatch");

    /**
     * An IAM policy-evaluation denial names the refused action, as in {@code User: arn:aws:sts::…:assumed-role/r/s is not
     * authorized to perform: kms:Decrypt on resource: arn:aws:kms:…}. Only a well-formed {@code service:Action} is
     * captured, and only up to a length no IAM action reaches, so nothing else from the store's message (principal,
     * resource ARN) can be echoed.
     */
    private static final Pattern DENIED_ACTION = Pattern.compile("not authorized to perform: ([a-z0-9-]{1,64}:[A-Z][A-Za-z0-9]{0,127})\\b");

    private S3FailureDetail() {}

    @Nullable
    static S3Exception findCredentialsExpired(Throwable cause) {
        Throwable current = cause;
        for (int depth = 0; depth < MAX_CAUSE_DEPTH && current != null; depth++) {
            if (current instanceof S3Exception s3 && isCredentialsExpiredCode(s3)) {
                return s3;
            }
            Throwable next = current.getCause();
            if (next == null || next == current) {
                break;
            }
            current = next;
        }
        return null;
    }

    static boolean isCredentialsExpiredCode(S3Exception s3) {
        String code = errorCode(s3);
        return code != null && CREDENTIALS_EXPIRED_CODES.contains(code);
    }

    /**
     * The IAM action a 403 says the principal is not authorized to perform, or {@code null} when the store's message
     * names none. S3 answers a policy denial and a KMS key the principal may not use both with 403 {@code AccessDenied};
     * only the message names the refused action.
     */
    @Nullable
    static String deniedAction(S3Exception s3) {
        if (s3.statusCode() != 403 || s3.awsErrorDetails() == null) {
            return null;
        }
        String message = s3.awsErrorDetails().errorMessage();
        if (message == null) {
            return null;
        }
        Matcher matcher = DENIED_ACTION.matcher(message);
        return matcher.find() ? matcher.group(1) : null;
    }

    static boolean isCredentialsRejected(S3Exception s3) {
        String code = errorCode(s3);
        return s3.statusCode() == 403 && code != null && CREDENTIALS_REJECTED_CODES.contains(code);
    }

    /**
     * The remedy an archived object's refusal carries, shared by the refused-GET path and the HEAD path so one
     * wording serves both. {@code tier} is {@link #archivedTier}'s rendering, or empty when none is known.
     */
    static String archivedRemedy(String tier) {
        return tier.isEmpty()
            ? "Restore it, or wait for a restore in progress to finish, before reading it."
            : "It is in " + tier + "; restore it, or wait for a restore in progress to finish, before reading it.";
    }

    /**
     * Storage classes whose objects must be restored before a GET will serve them. {@code GLACIER_IR} is not one:
     * Instant Retrieval serves a GET without a restore.
     */
    private static final Set<StorageClass> ARCHIVE_STORAGE_CLASSES = Set.of(StorageClass.GLACIER, StorageClass.DEEP_ARCHIVE);

    /** Intelligent-Tiering archive tiers that behave the same way, reported separately from the storage class. */
    private static final Set<ArchiveStatus> ARCHIVE_ACCESS_TIERS = Set.of(ArchiveStatus.ARCHIVE_ACCESS, ArchiveStatus.DEEP_ARCHIVE_ACCESS);

    /**
     * Whether a HEAD response describes an object a GET would refuse with {@code InvalidObjectState}. Three
     * headers decide it, and reading fewer gets it wrong in both directions: an Intelligent-Tiering object names
     * {@code INTELLIGENT_TIERING} as its class and carries the archive state in {@code x-amz-archive-status}, and
     * a restored object still names its archive class while {@code x-amz-restore} reports the restore finished.
     */
    static boolean isArchivedHeadResponse(@Nullable StorageClass storageClass, @Nullable ArchiveStatus archiveStatus, String restore) {
        // Both are null for an object whose class S3 does not report, and Set.of().contains(null) throws.
        boolean archived = (storageClass != null && ARCHIVE_STORAGE_CLASSES.contains(storageClass))
            || (archiveStatus != null && ARCHIVE_ACCESS_TIERS.contains(archiveStatus));
        return archived && restoreFinished(restore) == false;
    }

    /** {@code x-amz-restore} reports {@code ongoing-request="false"} once a restored copy is available. */
    private static boolean restoreFinished(String restore) {
        return restore != null && restore.contains("ongoing-request=\"false\"");
    }

    /** The class and tier a HEAD response reports, rendered as the refused-GET path below renders its own. */
    static String archivedTier(@Nullable StorageClass storageClass, @Nullable ArchiveStatus archiveStatus) {
        return renderTier(
            knownName(storageClass, StorageClass.UNKNOWN_TO_SDK_VERSION),
            knownName(archiveStatus, ArchiveStatus.UNKNOWN_TO_SDK_VERSION)
        );
    }

    /**
     * True when S3 refused the read because the object is in an archive storage class or tier and is not restored. The
     * SDK builds an {@link InvalidObjectStateException} for that code whichever store sent it; the code is what is keyed
     * on, whatever the status, since no other condition shares it.
     */
    static boolean isArchived(S3Exception s3) {
        return "InvalidObjectState".equals(errorCode(s3));
    }

    /**
     * The storage class and access tier an archived object's refusal reports, as in {@code storage class [GLACIER]}, or
     * an empty string when it reports neither. Only values the SDK knows are rendered, never the store's raw text.
     */
    static String archivedTier(S3Exception s3) {
        if (s3 instanceof InvalidObjectStateException archived) {
            return renderTier(
                knownName(archived.storageClass(), StorageClass.UNKNOWN_TO_SDK_VERSION),
                knownName(archived.accessTier(), IntelligentTieringAccessTier.UNKNOWN_TO_SDK_VERSION)
            );
        }
        return "";
    }

    /**
     * One rendering for both paths, so the same object reads the same way whichever request reported it. Either
     * part may be absent: an archived object in a plain archive class reports no access tier, and an
     * Intelligent-Tiering one reports a class that is not itself an archive class.
     */
    private static String renderTier(@Nullable String storageClass, @Nullable String accessTier) {
        StringBuilder sb = new StringBuilder();
        if (storageClass != null) {
            sb.append("storage class [").append(storageClass).append(']');
        }
        if (accessTier != null) {
            sb.append(sb.isEmpty() ? "" : " and ").append("access tier [").append(accessTier).append(']');
        }
        return sb.toString();
    }

    /**
     * The SDK's name for a value it knows, or {@code null} for one it does not. A store may answer with a class or
     * tier this SDK has never heard of, and echoing it would put the store's own text in our message.
     */
    @Nullable
    private static <E extends Enum<E>> String knownName(@Nullable E value, E unknown) {
        return value == null || value == unknown ? null : value.toString();
    }

    /** True when the response carried no error code, as a bodyless HEAD response cannot. */
    static boolean lacksErrorCode(S3Exception s3) {
        String code = errorCode(s3);
        return code == null || code.isEmpty();
    }

    @Nullable
    private static String errorCode(S3Exception s3) {
        return s3.awsErrorDetails() != null ? s3.awsErrorDetails().errorCode() : null;
    }

    // action is the gerund after "Session credentials expired or invalid"
    // ("reading [s3://…]", "listing objects…").
    @Nullable
    static ExternalCredentialsExpiredException expired(Throwable cause, String action) {
        S3Exception s3 = findCredentialsExpired(cause);
        if (s3 == null) {
            return null;
        }
        return new ExternalCredentialsExpiredException(StoragePath.NONE, of(s3), "", cause);
    }

    static String of(Throwable cause) {
        if (cause instanceof S3Exception s3) {
            String code = errorCode(s3);
            return code == null || code.isEmpty() ? "HTTP " + s3.statusCode() : "HTTP " + s3.statusCode() + " " + code;
        }
        // Use the class name for non-S3Exception causes: getMessage() may embed a full storage URI.
        return cause.getClass().getSimpleName();
    }
}
