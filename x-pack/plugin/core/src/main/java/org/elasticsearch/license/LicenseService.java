/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.license;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.protocol.xpack.XPackInfoResponse;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ReadOnly service to get the current license. Consumers should generally not need to read the license directly and should instead
 * prefer {@link XPackLicenseState} to check if a feature is allowed by the of license. This interface is not intended to be implemented
 * and alternative implementations
 */
public interface LicenseService extends LifecycleComponent {

    // should prefer getXPackLicenseState
    License getLicense();

    // Read the license from a provided Metadata. Implementations not backed by {@link ClusterState} should not implement.
    default License getLicense(Metadata metaData) {
        return getLicense();
    }

    Setting<License.LicenseType> SELF_GENERATED_LICENSE_TYPE = new Setting<>(
        "xpack.license.self_generated.type",
        License.LicenseType.BASIC.getTypeName(),
        (s) -> {
            final License.LicenseType type = License.LicenseType.parse(s);
            return SelfGeneratedLicense.validateSelfGeneratedType(type);
        },
        Setting.Property.NodeScope
    );
    List<License.LicenseType> ALLOWABLE_UPLOAD_TYPES = getAllowableUploadTypes();
    Setting<List<License.LicenseType>> ALLOWED_LICENSE_TYPES_SETTING = Setting.listSetting(
        "xpack.license.upload.types",
        ALLOWABLE_UPLOAD_TYPES.stream().map(License.LicenseType::getTypeName).toList(),
        License.LicenseType::parse,
        LicenseService::validateUploadTypesSetting,
        Setting.Property.NodeScope
    );
    // pkg private for tests
    TimeValue NON_BASIC_SELF_GENERATED_LICENSE_DURATION = TimeValue.timeValueHours(30 * 24);
    Set<License.LicenseType> VALID_TRIAL_TYPES = Set.of(
        License.LicenseType.GOLD,
        License.LicenseType.PLATINUM,
        License.LicenseType.ENTERPRISE,
        License.LicenseType.TRIAL
    );
    /**
     * Period before the license expires when warning starts being added to the response header
     */
    TimeValue LICENSE_EXPIRATION_WARNING_PERIOD = TimeValue.timeValueDays(7);
    long BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS = XPackInfoResponse.BASIC_SELF_GENERATED_LICENSE_EXPIRATION_MILLIS;
    /**
     * Max number of nodes licensed by generated trial license
     */
    int SELF_GENERATED_LICENSE_MAX_NODES = 1000;
    int SELF_GENERATED_LICENSE_MAX_RESOURCE_UNITS = SELF_GENERATED_LICENSE_MAX_NODES;

    static List<License.LicenseType> getAllowableUploadTypes() {
        return Stream.of(License.LicenseType.values()).filter(t -> t != License.LicenseType.BASIC).toList();
    }

    static void validateUploadTypesSetting(List<License.LicenseType> value) {
        if (ALLOWABLE_UPLOAD_TYPES.containsAll(value) == false) {
            throw new IllegalArgumentException(
                "Invalid value ["
                    + value.stream().map(License.LicenseType::getTypeName).collect(Collectors.joining(","))
                    + "] for "
                    + ALLOWED_LICENSE_TYPES_SETTING.getKey()
                    + ", allowed values are ["
                    + ALLOWABLE_UPLOAD_TYPES.stream().map(License.LicenseType::getTypeName).collect(Collectors.joining(","))
                    + "]"
            );
        }
    }

    interface MutableLicense extends LicenseService, LifecycleComponent {

        void updateXPackLicenseState(License license);

        void registerLicense(PutLicenseRequest request, ActionListener<PutLicenseResponse> listener);

        void removeLicense(ActionListener<? extends AcknowledgedResponse> listener);

        /**
         * @return true if the license was found to be expired, false otherwise. If license is null - return false.
         */
        boolean maybeExpireLicense(License license);

        void startBasicLicense(PostStartBasicRequest request, ActionListener<PostStartBasicResponse> listener);

        void startTrialLicense(PostStartTrialRequest request, ActionListener<PostStartTrialResponse> listener);

    }
}
