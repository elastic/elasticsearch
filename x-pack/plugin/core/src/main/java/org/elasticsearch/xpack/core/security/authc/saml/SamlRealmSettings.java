/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.authc.saml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.xpack.core.security.authc.support.DelegatedAuthorizationSettings;
import org.elasticsearch.xpack.core.ssl.SSLConfigurationSettings;
import org.elasticsearch.xpack.core.ssl.X509KeyPairSettings;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

public class SamlRealmSettings {

    public static final String TYPE = "saml";
    private static final String TRANSIENT_NAMEID_FORMAT = "urn:oasis:names:tc:SAML:2.0:nameid-format:transient";

    // these settings will be used under the prefix xpack.security.authc.realms.REALM_NAME.
    private static final String IDP_METADATA_SETTING_PREFIX = "idp.metadata.";

    public static final Setting<String> IDP_ENTITY_ID = Setting.simpleString("idp.entity_id", Setting.Property.NodeScope);
    public static final Setting<String> IDP_METADATA_PATH
            = Setting.simpleString(IDP_METADATA_SETTING_PREFIX + "path", Setting.Property.NodeScope);
    public static final Setting<TimeValue> IDP_METADATA_HTTP_REFRESH
            = Setting.timeSetting(IDP_METADATA_SETTING_PREFIX + "http.refresh", TimeValue.timeValueHours(1), Setting.Property.NodeScope);
    public static final Setting<Boolean> IDP_SINGLE_LOGOUT = Setting.boolSetting("idp.use_single_logout", true, Setting.Property.NodeScope);

    public static final Setting<String> SP_ENTITY_ID = Setting.simpleString("sp.entity_id", Setting.Property.NodeScope);
    public static final Setting<String> SP_ACS = Setting.simpleString("sp.acs", Setting.Property.NodeScope);
    public static final Setting<String> SP_LOGOUT = Setting.simpleString("sp.logout", Setting.Property.NodeScope);

    public static final Setting<String> NAMEID_FORMAT = new Setting<>("nameid_format", s -> TRANSIENT_NAMEID_FORMAT, Function.identity(),
            Setting.Property.NodeScope);
    public static final Setting<Boolean> NAMEID_ALLOW_CREATE = Setting.boolSetting("nameid.allow_create", false,
            Setting.Property.NodeScope);
    public static final Setting<String> NAMEID_SP_QUALIFIER = Setting.simpleString("nameid.sp_qualifier", Setting.Property.NodeScope);

    public static final Setting<Boolean> FORCE_AUTHN = Setting.boolSetting("force_authn", false, Setting.Property.NodeScope);
    public static final Setting<Boolean> POPULATE_USER_METADATA = Setting.boolSetting("populate_user_metadata", true,
            Setting.Property.NodeScope);

    public static final AttributeSetting PRINCIPAL_ATTRIBUTE = new AttributeSetting("principal");
    public static final AttributeSetting GROUPS_ATTRIBUTE = new AttributeSetting("groups");
    public static final AttributeSetting DN_ATTRIBUTE = new AttributeSetting("dn");
    public static final AttributeSetting NAME_ATTRIBUTE = new AttributeSetting("name");
    public static final AttributeSetting MAIL_ATTRIBUTE = new AttributeSetting("mail");

    public static final X509KeyPairSettings ENCRYPTION_SETTINGS = new X509KeyPairSettings("encryption.", false);
    public static final Setting<String> ENCRYPTION_KEY_ALIAS =
            Setting.simpleString("encryption.keystore.alias", Setting.Property.NodeScope);

    public static final X509KeyPairSettings SIGNING_SETTINGS = new X509KeyPairSettings("signing.", false);
    public static final Setting<String> SIGNING_KEY_ALIAS =
            Setting.simpleString("signing.keystore.alias", Setting.Property.NodeScope);
    public static final Setting<List<String>> SIGNING_MESSAGE_TYPES = Setting.listSetting("signing.saml_messages",
            Collections.singletonList("*"), Function.identity(), Setting.Property.NodeScope);
    public static final Setting<List<String>> REQUESTED_AUTHN_CONTEXT_CLASS_REF = Setting.listSetting("req_authn_context_class_ref",
        Collections.emptyList(), Function.identity(),Setting.Property.NodeScope);
    public static final Setting<TimeValue> CLOCK_SKEW = Setting.positiveTimeSetting("allowed_clock_skew", TimeValue.timeValueMinutes(3),
            Setting.Property.NodeScope);

    public static final String SSL_PREFIX = "ssl.";

    private SamlRealmSettings() {
    }

    /**
     * @return The {@link Setting setting configuration} for this realm type
     */
    public static Set<Setting<?>> getSettings() {
        final Set<Setting<?>> set = Sets.newHashSet(IDP_ENTITY_ID, IDP_METADATA_PATH, IDP_SINGLE_LOGOUT,
                SP_ENTITY_ID, SP_ACS, SP_LOGOUT,
                NAMEID_FORMAT, NAMEID_ALLOW_CREATE, NAMEID_SP_QUALIFIER, FORCE_AUTHN,
                POPULATE_USER_METADATA, CLOCK_SKEW,
            ENCRYPTION_KEY_ALIAS, SIGNING_KEY_ALIAS, SIGNING_MESSAGE_TYPES, REQUESTED_AUTHN_CONTEXT_CLASS_REF);
        set.addAll(ENCRYPTION_SETTINGS.getAllSettings());
        set.addAll(SIGNING_SETTINGS.getAllSettings());
        set.addAll(SSLConfigurationSettings.withPrefix(SSL_PREFIX).getAllSettings());
        set.addAll(PRINCIPAL_ATTRIBUTE.settings());
        set.addAll(GROUPS_ATTRIBUTE.settings());
        set.addAll(DN_ATTRIBUTE.settings());
        set.addAll(NAME_ATTRIBUTE.settings());
        set.addAll(MAIL_ATTRIBUTE.settings());
        set.addAll(DelegatedAuthorizationSettings.getSettings());
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

        private final Setting<String> attribute;
        private final Setting<String> pattern;

        public AttributeSetting(String name) {
            attribute = Setting.simpleString(ATTRIBUTES_PREFIX + name, Setting.Property.NodeScope);
            pattern = Setting.simpleString(ATTRIBUTE_PATTERNS_PREFIX + name, Setting.Property.NodeScope);
        }

        public Collection<Setting<?>> settings() {
            return Arrays.asList(getAttribute(), getPattern());
        }

        public String name() {
            return getAttribute().getKey();
        }

        public Setting<String> getAttribute() {
            return attribute;
        }

        public Setting<String> getPattern() {
            return pattern;
        }
    }
}
