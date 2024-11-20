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

import static org.elasticsearch.xpack.core.security.authc.support.SecuritySettingsUtil.verifyNonNullNotEmpty;

public class SamlRealmSettings {

    public static final String TYPE = "saml";

    // these settings will be used under the prefix xpack.security.authc.realms.REALM_NAME.
    private static final String IDP_METADATA_SETTING_PREFIX = "idp.metadata.";

    public static final Setting.AffixSetting<String> IDP_ENTITY_ID = RealmSettings.simpleString(
        TYPE,
        "idp.entity_id",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<String> IDP_METADATA_PATH = RealmSettings.simpleString(
        TYPE,
        IDP_METADATA_SETTING_PREFIX + "path",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<TimeValue> IDP_METADATA_HTTP_REFRESH = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        IDP_METADATA_SETTING_PREFIX + "http.refresh",
        key -> Setting.timeSetting(key, TimeValue.timeValueHours(1), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> IDP_METADATA_HTTP_MIN_REFRESH = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        IDP_METADATA_SETTING_PREFIX + "http.minimum_refresh",
        key -> Setting.timeSetting(key, TimeValue.timeValueMinutes(5), TimeValue.timeValueMillis(500), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Boolean> IDP_METADATA_HTTP_FAIL_ON_ERROR = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        IDP_METADATA_SETTING_PREFIX + "http.fail_on_error",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Boolean> IDP_SINGLE_LOGOUT = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "idp.use_single_logout",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<String> SP_ENTITY_ID = RealmSettings.simpleString(
        TYPE,
        "sp.entity_id",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<String> SP_ACS = RealmSettings.simpleString(TYPE, "sp.acs", Setting.Property.NodeScope);
    public static final Setting.AffixSetting<String> SP_LOGOUT = RealmSettings.simpleString(TYPE, "sp.logout", Setting.Property.NodeScope);

    public static final Setting.AffixSetting<String> NAMEID_FORMAT = RealmSettings.simpleString(
        TYPE,
        "nameid_format",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Boolean> NAMEID_ALLOW_CREATE = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "nameid.allow_create",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );
    public static final Setting.AffixSetting<String> NAMEID_SP_QUALIFIER = RealmSettings.simpleString(
        TYPE,
        "nameid.sp_qualifier",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<Boolean> FORCE_AUTHN = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "force_authn",
        key -> Setting.boolSetting(key, false, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<Boolean> POPULATE_USER_METADATA = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "populate_user_metadata",
        key -> Setting.boolSetting(key, true, Setting.Property.NodeScope)
    );

    public static final AttributeSetting PRINCIPAL_ATTRIBUTE = new AttributeSetting("principal");
    public static final AttributeSettingWithDelimiter GROUPS_ATTRIBUTE = new AttributeSettingWithDelimiter("groups");
    public static final AttributeSetting DN_ATTRIBUTE = new AttributeSetting("dn");
    public static final AttributeSetting NAME_ATTRIBUTE = new AttributeSetting("name");
    public static final AttributeSetting MAIL_ATTRIBUTE = new AttributeSetting("mail");

    public static final String ENCRYPTION_SETTING_KEY = "encryption.";
    public static final Setting.AffixSetting<String> ENCRYPTION_KEY_ALIAS = RealmSettings.simpleString(
        TYPE,
        ENCRYPTION_SETTING_KEY + "keystore.alias",
        Setting.Property.NodeScope
    );

    public static final String SIGNING_SETTING_KEY = "signing.";
    public static final Setting.AffixSetting<String> SIGNING_KEY_ALIAS = RealmSettings.simpleString(
        TYPE,
        SIGNING_SETTING_KEY + "keystore.alias",
        Setting.Property.NodeScope
    );

    public static final Setting.AffixSetting<List<String>> SIGNING_MESSAGE_TYPES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "signing.saml_messages",
        key -> Setting.stringListSetting(key, List.of("*"), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> REQUESTED_AUTHN_CONTEXT_CLASS_REF = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "req_authn_context_class_ref",
        key -> Setting.stringListSetting(key, Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<TimeValue> CLOCK_SKEW = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "allowed_clock_skew",
        key -> Setting.positiveTimeSetting(key, TimeValue.timeValueMinutes(3), Setting.Property.NodeScope)
    );

    public static final Setting.AffixSetting<List<String>> EXCLUDE_ROLES = Setting.affixKeySetting(
        RealmSettings.realmSettingPrefix(TYPE),
        "exclude_roles",
        key -> Setting.stringListSetting(key, new Setting.Validator<>() {

            @Override
            public void validate(List<String> excludedRoles) {
                excludedRoles.forEach(excludedRole -> verifyNonNullNotEmpty(key, excludedRole));
            }

            @Override
            public void validate(List<String> excludedRoles, Map<Setting<?>, Object> settings) {
                if (false == excludedRoles.isEmpty()) {
                    final String namespace = EXCLUDE_ROLES.getNamespace(EXCLUDE_ROLES.getConcreteSetting(key));
                    final Setting<List<String>> authorizationRealmsSetting = DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(TYPE)
                        .getConcreteSettingForNamespace(namespace);
                    @SuppressWarnings("unchecked")
                    final List<String> authorizationRealms = (List<String>) settings.get(authorizationRealmsSetting);
                    if (authorizationRealms != null && false == authorizationRealms.isEmpty()) {
                        throw new SettingsException(
                            "Setting ["
                                + EXCLUDE_ROLES.getConcreteSettingForNamespace(namespace).getKey()
                                + "] is not permitted when setting ["
                                + authorizationRealmsSetting.getKey()
                                + "] is configured."
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final String namespace = EXCLUDE_ROLES.getNamespace(EXCLUDE_ROLES.getConcreteSetting(key));
                final List<Setting<?>> settings = List.of(
                    DelegatedAuthorizationSettings.AUTHZ_REALMS.apply(TYPE).getConcreteSettingForNamespace(namespace)
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
    public static Set<Setting.AffixSetting<?>> getSettings() {
        final Set<Setting.AffixSetting<?>> set = Sets.newHashSet(
            IDP_ENTITY_ID,
            IDP_METADATA_PATH,
            IDP_METADATA_HTTP_REFRESH,
            IDP_METADATA_HTTP_MIN_REFRESH,
            IDP_METADATA_HTTP_FAIL_ON_ERROR,
            IDP_SINGLE_LOGOUT,
            SP_ENTITY_ID,
            SP_ACS,
            SP_LOGOUT,
            NAMEID_FORMAT,
            NAMEID_ALLOW_CREATE,
            NAMEID_SP_QUALIFIER,
            FORCE_AUTHN,
            POPULATE_USER_METADATA,
            CLOCK_SKEW,
            ENCRYPTION_KEY_ALIAS,
            SIGNING_KEY_ALIAS,
            SIGNING_MESSAGE_TYPES,
            REQUESTED_AUTHN_CONTEXT_CLASS_REF
        );
        set.addAll(X509KeyPairSettings.affix(RealmSettings.realmSettingPrefix(TYPE), ENCRYPTION_SETTING_KEY, false));
        set.addAll(X509KeyPairSettings.affix(RealmSettings.realmSettingPrefix(TYPE), SIGNING_SETTING_KEY, false));
        set.addAll(SSLConfigurationSettings.getRealmSettings(TYPE));
        set.addAll(PRINCIPAL_ATTRIBUTE.settings());
        set.addAll(GROUPS_ATTRIBUTE.settings());
        set.addAll(DN_ATTRIBUTE.settings());
        set.addAll(NAME_ATTRIBUTE.settings());
        set.addAll(MAIL_ATTRIBUTE.settings());

        set.addAll(DelegatedAuthorizationSettings.getSettings(TYPE));
        set.addAll(RealmSettings.getStandardSettings(TYPE));
        return set;
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

        public AttributeSetting(String name) {
            attribute = RealmSettings.simpleString(TYPE, ATTRIBUTES_PREFIX + name, Setting.Property.NodeScope);
            pattern = RealmSettings.simpleString(TYPE, ATTRIBUTE_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
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

        public AttributeSettingWithDelimiter(String name) {
            this.attributeSetting = new AttributeSetting(name);
            this.delimiter = RealmSettings.simpleString(TYPE, ATTRIBUTE_DELIMITERS_PREFIX + name, Setting.Property.NodeScope);
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
