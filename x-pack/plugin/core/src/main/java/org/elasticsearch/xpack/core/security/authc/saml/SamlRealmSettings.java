/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authc.saml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.authc.support.SecuritySettingsUtil.verifyNonNullNotEmpty;

public class SamlRealmSettings {

    // these settings will be used under the prefix xpack.security.authc.realms.REALM_NAME.
    private static final String IDP_METADATA_SETTING_PREFIX = "idp.metadata.";

    public static final Function<String, Setting.AffixSetting<String>> IDP_ENTITY_ID = (type) -> RealmSettings.simpleString(
        type,
        "idp.entity_id",
        Setting.Property.NodeScope
    );

    public static final Function<String, Setting.AffixSetting<String>> IDP_METADATA_PATH = (type) -> RealmSettings.simpleString(
        type,
        IDP_METADATA_SETTING_PREFIX + "path",
        Setting.Property.NodeScope
    );

    public static final Function<String, Setting.AffixSetting<TimeValue>> IDP_METADATA_HTTP_REFRESH = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        IDP_METADATA_SETTING_PREFIX + "http.refresh",
        key -> Setting.timeSetting(key, TimeValue.timeValueHours(1), Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<TimeValue>> IDP_METADATA_HTTP_MIN_REFRESH = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        IDP_METADATA_SETTING_PREFIX + "http.minimum_refresh",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(5), TimeValue.timeValueMillis(500), Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<Boolean>> IDP_METADATA_HTTP_FAIL_ON_ERROR = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        IDP_METADATA_SETTING_PREFIX + "http.fail_on_error",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<Boolean>> IDP_SINGLE_LOGOUT = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "idp.use_single_logout",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<String>> NAMEID_FORMAT = (type) -> RealmSettings.simpleString(
        type,
        "nameid_format",
        Setting.Property.NodeScope
    );

    public static final Function<String, Setting.AffixSetting<Boolean>> NAMEID_ALLOW_CREATE = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "nameid.allow_create",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );
    public static final Function<String, Setting.AffixSetting<String>> NAMEID_SP_QUALIFIER = (type) -> RealmSettings.simpleString(
        type,
        "nameid.sp_qualifier",
        Setting.Property.NodeScope
    );

    public static final Function<String, Setting.AffixSetting<Boolean>> FORCE_AUTHN = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "force_authn",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<Boolean>> POPULATE_USER_METADATA = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );

    public static final Function<String, AttributeSetting> PRINCIPAL_ATTRIBUTE = (type) -> new AttributeSetting(type, "principal");
    public static final Function<String, AttributeSettingWithDelimiter> GROUPS_ATTRIBUTE = (type) -> new AttributeSettingWithDelimiter(
        type,
        "groups"
    );
    public static final Function<String, AttributeSetting> DN_ATTRIBUTE = (type) -> new AttributeSetting(type, "dn");
    public static final Function<String, AttributeSetting> NAME_ATTRIBUTE = (type) -> new AttributeSetting(type, "name");
    public static final Function<String, AttributeSetting> MAIL_ATTRIBUTE = (type) -> new AttributeSetting(type, "mail");

    public static final String ENCRYPTION_SETTING_KEY = "encryption.";
    public static final Function<String, Setting.AffixSetting<String>> ENCRYPTION_KEY_ALIAS = (type) -> RealmSettings.simpleString(
        type,
        ENCRYPTION_SETTING_KEY + "keystore.alias",
        Setting.Property.NodeScope
    );

    public static final String SIGNING_SETTING_KEY = "signing.";
    public static final Function<String, Setting.AffixSetting<String>> SIGNING_KEY_ALIAS = (type) -> RealmSettings.simpleString(
        type,
        SIGNING_SETTING_KEY + "keystore.alias",
        Setting.Property.NodeScope
    );

    public static final Function<String, Setting.AffixSetting<List<String>>> SIGNING_MESSAGE_TYPES = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "signing.saml_messages",
        key -> Setting.stringListSetting(key, List.of("*"), Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<List<String>>> REQUESTED_AUTHN_CONTEXT_CLASS_REF = (type) -> Setting
        .affixKeySetting(
            RealmSettings.realmSettingPrefix(type),
            "req_authn_context_class_ref",
            key -> Setting.stringListSetting(key, Setting.Property.NodeScope)
        );

    public static final Function<String, Setting.AffixSetting<TimeValue>> CLOCK_SKEW = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "allowed_clock_skew",
        key -> Setting.positiveTimeSetting(key, TimeValue.timeValueMinutes(3), Setting.Property.NodeScope)
    );

    public static final Function<String, Setting.AffixSetting<List<String>>> EXCLUDE_ROLES = (type) -> Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(type),
        "exclude_roles",
        (namespace, key) -> Setting.stringListSetting(key, new Setting.Validator<>() {
            @Override
            public void validate(List<String> excludedRoles) {
                excludedRoles.forEach(excludedRole -> verifyNonNullNotEmpty(key, excludedRole));
            }

            @Override
            public void validate(List<String> excludedRoles, Map<Setting<?>, Object> settings) {
                if (false == excludedRoles.isEmpty()) {
                    final Setting<List<String>> authorizationRealmsSetting = DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(type)
                        .getConcreteSettingForNamespace(namespace);
                    @SuppressWarnings("unchecked")
                    final List<String> authorizationRealms = (List<String>) settings.get(authorizationRealmsSetting);
                    if (authorizationRealms != null && false == authorizationRealms.isEmpty()) {
                        throw new SettingsException(
                            "Setting ["
                                + key
                                + "] is not permitted when setting ["
                                + authorizationRealmsSetting.getKey()
                                + "] is configured."
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(
                    DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(type).getConcreteSettingForNamespace(namespace)
                );
                return settings.iterator();
            }
        }, Setting.Property.NodeScope)
    );

    public static final String SSL_PREFIX = "ssl.";

    private SamlRealmSettings() {}

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting.AffixSetting<?>> getSettings(String type) {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            IDP_ENTITY_ID.apply(type),
            IDP_METADATA_PATH.apply(type),
            IDP_METADATA_HTTP_REFRESH.apply(type),
            IDP_METADATA_HTTP_MIN_REFRESH.apply(type),
            IDP_METADATA_HTTP_FAIL_ON_ERROR.apply(type),
            IDP_SINGLE_LOGOUT.apply(type),
            NAMEID_FORMAT.apply(type),
            NAMEID_ALLOW_CREATE.apply(type),
            NAMEID_SP_QUALIFIER.apply(type),
            FORCE_AUTHN.apply(type),
            POPULATE_USER_METADATA.apply(type),
            CLOCK_SKEW.apply(type),
            ENCRYPTION_KEY_ALIAS.apply(type),
            SIGNING_KEY_ALIAS.apply(type),
            SIGNING_MESSAGE_TYPES.apply(type),
            REQUESTED_AUTHN_CONTEXT_CLASS_REF.apply(type)
        );

        set.addAll(X509KeyPairSettings.affix(RealmSettings.realmSettingPrefix(type), ENCRYPTION_SETTING_KEY, false));
        set.addAll(X509KeyPairSettings.affix(RealmSettings.realmSettingPrefix(type), SIGNING_SETTING_KEY, false));
        set.addAll(SSLConfigurationSettings.getRealmSettings(type));
        set.addAll(PRINCIPAL_ATTRIBUTE.apply(type).settings());
        set.addAll(GROUPS_ATTRIBUTE.apply(type).settings());
        set.addAll(DN_ATTRIBUTE.apply(type).settings());
        set.addAll(NAME_ATTRIBUTE.apply(type).settings());
        set.addAll(MAIL_ATTRIBUTE.apply(type).settings());

        set.addAll(DelegatedAuthorizationSettings.getSettings(type));
        set.addAll(RealmSettings.getStandardSettings(type));
        return set;
    }

    public record UserAttributeNameConfiguration(String principal, String dn, String name, String mail) {
        public static UserAttributeNameConfiguration fromConfig(RealmConfig config) {
            return new UserAttributeNameConfiguration(
                PRINCIPAL_ATTRIBUTE.apply(config.type()).name(config),
                DN_ATTRIBUTE.apply(config.type()).name(config),
                NAME_ATTRIBUTE.apply(config.type()).name(config),
                MAIL_ATTRIBUTE.apply(config.type()).name(config)
            );
        }
    }

    /**
     * The SAML realm offers a number of settings that rely on attributes that are populate by the Identity Provider in the SAML Response.
     * Each attribute has 2 settings:
     * <ul>
     * <li>The name of the SAML attribute to use</li>
     * <li>A java pattern (regex) to apply to that attribute value in order to extract the substring that should be used.</li>
     * </ul>
     * For example, the Elasticsearch User Principal could be configured to come from the SAML "mail" attribute, and extract only the
     * local-port of the user's email address (i.e. the name before the '@').
     * This class encapsulates those 2 settings.
     */
    public static final class AttributeSetting {
        public static final String ATTRIBUTES_PREFIX = "attributes.";
        public static final String ATTRIBUTE_PATTERNS_PREFIX = "attribute_patterns.";

        private final Setting.AffixSetting<String> attribute;
        private final Setting.AffixSetting<String> pattern;

        public AttributeSetting(String type, String name) {
            attribute = RealmSettings.simpleString(type, ATTRIBUTES_PREFIX + name, Setting.Property.NodeScope);
            pattern = RealmSettings.simpleString(type, ATTRIBUTE_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
        }

        public Collection<Setting.AffixSetting<?>> settings() {
            return Arrays.asList(getAttribute(), getPattern());
        }

        public String name(RealmConfig config) {
            return getAttribute().getConcreteSettingForNamespace(config.name()).getKey();
        }

        public Setting.AffixSetting<String> getAttribute() {
            return attribute;
        }

        public Setting.AffixSetting<String> getPattern() {
            return pattern;
        }
    }

    /**
     * The SAML realm offers a setting where a multivalued attribute can be configured to have a delimiter for its values, for the case
     * when all values are provided in a single string item, separated by a delimiter.
     * As in {@link AttributeSetting} there are two settings:
     * <ul>
     * <li>The name of the SAML attribute to use</li>
     * <li>A delimiter to apply to that attribute value in order to extract the substrings that should be used.</li>
     * </ul>
     * For example, the Elasticsearch Group could be configured to come from the SAML "department" attribute, where all groups are provided
     * as a csv value in a single list item.
     */
    public static final class AttributeSettingWithDelimiter {
        public static final String ATTRIBUTE_DELIMITERS_PREFIX = "attribute_delimiters.";
        private final Setting.AffixSetting<String> delimiter;
        private final AttributeSetting attributeSetting;

        public AttributeSetting getAttributeSetting() {
            return attributeSetting;
        }

        public AttributeSettingWithDelimiter(String type, String name) {
            this.attributeSetting = new AttributeSetting(type, name);
            this.delimiter = RealmSettings.simpleString(type, ATTRIBUTE_DELIMITERS_PREFIX + name, Setting.Property.NodeScope);
        }

        public Setting.AffixSetting<String> getDelimiter() {
            return this.delimiter;
        }

        public Collection<Setting.AffixSetting<?>> settings() {
            List<Setting.AffixSetting<?>> settings = new ArrayList<>(attributeSetting.settings());
            settings.add(getDelimiter());
            return settings;
        }
    }
}
