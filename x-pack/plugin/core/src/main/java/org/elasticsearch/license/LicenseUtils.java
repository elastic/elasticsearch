/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.License.LicenseType;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.protocol.xpack.license.LicenseStatus;
import org.elasticsearch.rest.RestStatus;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public class LicenseUtils {

    public static final String EXPIRED_FEATURE_METADATA = "es.license.expired.feature";

    public static String formatMillis(long millis) {
        // DateFormatters logs a warning about the pattern on COMPAT
        // this will be confusing to users, so call DateTimeFormatter directly instead
        return DateTimeFormatter.ofPattern("EEEE, MMMM dd, yyyy", Locale.ENGLISH)
            .format(Instant.ofEpochMilli(millis).atOffset(ZoneOffset.UTC));
    }

    /**
     * Exception to be thrown when a feature action requires a valid license, but license
     * has expired
     *
     * <code>feature</code> accessible through {@link #EXPIRED_FEATURE_METADATA} in the
     * exception's rest header
     */
    public static ElasticsearchSecurityException newComplianceException(String feature) {
        ElasticsearchSecurityException e = new ElasticsearchSecurityException(
            "current license is non-compliant for [{}]",
            RestStatus.FORBIDDEN,
            feature
        );
        e.addMetadata(EXPIRED_FEATURE_METADATA, feature);
        return e;
    }

    /**
     * Checks if a given {@link ElasticsearchSecurityException} refers to a feature that
     * requires a valid license, but the license has expired.
     */
    public static boolean isLicenseExpiredException(ElasticsearchSecurityException exception) {
        return (exception != null) && (exception.getMetadata(EXPIRED_FEATURE_METADATA) != null);
    }

    public static boolean licenseNeedsExtended(License license) {
        return LicenseType.isBasic(license.type())
            && getExpiryDate(license) != LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
    }

    /**
     * Checks if the signature of a self generated license with older version needs to be
     * recreated with the new key
     */
    public static boolean signatureNeedsUpdate(License license) {
        assert License.VERSION_ENTERPRISE == License.VERSION_CURRENT : "update this method when adding a new version";

        String typeName = license.type();
        return (LicenseType.isBasic(typeName) || LicenseType.isTrial(typeName))
            && license.version() < License.VERSION_CRYPTO_ALGORITHMS
            && getMaxCompatibleLicenseVersion() >= License.VERSION_CRYPTO_ALGORITHMS;// only upgrade signature when all nodes are ready to
                                                                                     // deserialize the new signature
    }

    /**
     * Gets the maximum license version this cluster is compatible with. This is semantically different from {@link License#VERSION_CURRENT}
     * as that field is the maximum that can be handled _by this node_, whereas this method determines the maximum license version
     * that can be handled _by this cluster_.
     */
    public static int getMaxCompatibleLicenseVersion() {
        assert License.VERSION_ENTERPRISE == License.VERSION_CURRENT : "update this method when adding a new version";
        return License.VERSION_ENTERPRISE;
    }

    /**
     * Gets the effective expiry date of the given license, including any overrides.
     */
    public static long getExpiryDate(License license) {
        String licenseUidHash = MessageDigests.toHexString(MessageDigests.sha256().digest(license.uid().getBytes(StandardCharsets.UTF_8)));
        return LicenseOverrides.overrideDateForLicense(licenseUidHash)
            .map(date -> date.toInstant().toEpochMilli())
            .orElse(license.expiryDate());
    }

    /**
     * Gets the current status of a license
     */
    public static LicenseStatus status(License license) {
        long now = System.currentTimeMillis();
        if (license.issueDate() > now) {
            return LicenseStatus.INVALID;
        } else if (getExpiryDate(license) < now) {
            return LicenseStatus.EXPIRED;
        }
        return LicenseStatus.ACTIVE;
    }

    /**
     * Derive the status from the {@link License} for use with {@link XPackLicenseState}
     * @param license The license used to derive the returned {@link XPackLicenseStatus}
     * @param clock The clock used for expiry checks. Will typically be Clock.systemUTC();
     * @return The status for use with {@link XPackLicenseState}
     */
    public static XPackLicenseStatus getXPackLicenseStatus(License license, Clock clock) {
        long now = clock.millis();
        Objects.requireNonNull(license, "license must not be null");
        Objects.requireNonNull(clock, "clock must not be null");
        if (license == LicensesMetadata.LICENSE_TOMBSTONE) {
            return new XPackLicenseStatus(License.OperationMode.MISSING, false, getExpiryWarning(LicenseUtils.getExpiryDate(license), now));
        } else {
            final boolean active;
            if (LicenseUtils.getExpiryDate(license) == LicenseSettings.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS) {
                active = true;
            } else {
                active = now >= license.issueDate() && now < LicenseUtils.getExpiryDate(license);
            }
            return new XPackLicenseStatus(license.operationMode(), active, getExpiryWarning(LicenseUtils.getExpiryDate(license), now));
        }
    }

    public static Map<String, String[]> getAckMessages(License newLicense, License currentLicense) {
        Map<String, String[]> acknowledgeMessages = new HashMap<>();
        if (License.isAutoGeneratedLicense(currentLicense.signature()) == false // current license is not auto-generated
            && currentLicense.issueDate() > newLicense.issueDate()) { // and has a later issue date
            acknowledgeMessages.put(
                "license",
                new String[] {
                    "The new license is older than the currently installed license. "
                        + "Are you sure you want to override the current license?" }
            );
        }
        XPackLicenseState.ACKNOWLEDGMENT_MESSAGES.forEach((feature, ackMessages) -> {
            String[] messages = ackMessages.apply(currentLicense.operationMode(), newLicense.operationMode());
            if (messages.length > 0) {
                acknowledgeMessages.put(feature, messages);
            }
        });
        return acknowledgeMessages;
    }

    public static String getExpiryWarning(long licenseExpiryDate, long currentTime) {
        final long diff = licenseExpiryDate - currentTime;
        if (LicenseSettings.LICENSE_EXPIRATION_WARNING_PERIOD.getMillis() > diff) {
            final long days = TimeUnit.MILLISECONDS.toDays(diff);
            final String expiryMessage = (days == 0 && diff > 0)
                ? "expires today"
                : (diff > 0
                    ? String.format(Locale.ROOT, "will expire in [%d] days", days)
                    : String.format(Locale.ROOT, "expired on [%s]", formatMillis(licenseExpiryDate)));
            return "Your license "
                + expiryMessage
                + ". "
                + "Contact your administrator or update your license for continued use of features";
        }
        return null;
    }
}
