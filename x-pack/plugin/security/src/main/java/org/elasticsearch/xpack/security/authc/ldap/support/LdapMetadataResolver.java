/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.SearchResultEntry;
import com.unboundid.ldap.sdk.SearchScope;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapMetadataResolverSettings;
import org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySIDUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySIDUtil.TOKEN_GROUPS;
import static org.elasticsearch.xpack.security.authc.ldap.ActiveDirectorySIDUtil.convertToString;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.OBJECT_CLASS_PRESENCE_FILTER;
import static org.elasticsearch.xpack.security.authc.ldap.support.LdapUtils.searchForEntry;

public class LdapMetadataResolver {
    private final String[] attributeNames;
    private final boolean ignoreReferralErrors;
    private final String fullNameAttributeName;
    private final String emailAttributeName;
    private final String[] allAttributeNamesToResolve;

    public LdapMetadataResolver(RealmConfig realmConfig, boolean ignoreReferralErrors) {
        this(
            realmConfig.getSetting(LdapMetadataResolverSettings.FULL_NAME_SETTING),
            realmConfig.getSetting(LdapMetadataResolverSettings.EMAIL_SETTING),
            realmConfig.getSetting(LdapMetadataResolverSettings.ADDITIONAL_METADATA_SETTING),
            ignoreReferralErrors
        );
    }

    LdapMetadataResolver(
        String fullNameAttributeName,
        String emailAttributeName,
        Collection<String> attributeNames,
        boolean ignoreReferralErrors
    ) {
        this.fullNameAttributeName = fullNameAttributeName;
        this.emailAttributeName = emailAttributeName;
        this.attributeNames = attributeNames.toArray(new String[attributeNames.size()]);
        this.ignoreReferralErrors = ignoreReferralErrors;
        this.allAttributeNamesToResolve = Stream.concat(
            Stream.of(this.attributeNames),
            Stream.of(this.fullNameAttributeName, this.emailAttributeName)
        ).distinct().toArray(String[]::new);

    }

    public String[] attributeNames() {
        return attributeNames;
    }

    public void resolve(
        LDAPInterface connection,
        String userDn,
        TimeValue timeout,
        Logger logger,
        Collection<Attribute> attributes,
        ActionListener<LdapMetadataResult> listener
    ) {
        if (Strings.isEmpty(this.fullNameAttributeName) && Strings.isEmpty(this.emailAttributeName) && this.attributeNames.length == 0) {
            listener.onResponse(LdapMetadataResult.EMPTY);
        } else if (attributes != null) {
            listener.onResponse(toLdapMetadataResult(name -> findAttribute(attributes, name)));
        } else {
            searchForEntry(
                connection,
                userDn,
                SearchScope.BASE,
                OBJECT_CLASS_PRESENCE_FILTER,
                Math.toIntExact(timeout.seconds()),
                ignoreReferralErrors,
                ActionListener.wrap((SearchResultEntry entry) -> {
                    if (entry == null) {
                        listener.onResponse(LdapMetadataResult.EMPTY);
                    } else {
                        listener.onResponse(toLdapMetadataResult(entry::getAttribute));
                    }
                }, listener::onFailure),
                allAttributeNamesToResolve
            );
        }
    }

    private static Attribute findAttribute(Collection<Attribute> attributes, String name) {
        return attributes.stream().filter(attr -> attr.getName().equals(name)).findFirst().orElse(null);
    }

    public static class LdapMetadataResult {

        public static final LdapMetadataResult EMPTY = new LdapMetadataResult(null, null, Map.of());

        private final String fullName;
        private final String email;
        private final Map<String, Object> metaData;

        public LdapMetadataResult(@Nullable String fullName, @Nullable String email, Map<String, Object> metaData) {
            this.fullName = fullName;
            this.email = email;
            this.metaData = metaData;
        }

        @Nullable
        public String getFullName() {
            return fullName;
        }

        @Nullable
        public String getEmail() {
            return email;
        }

        public Map<String, Object> getMetaData() {
            return metaData;
        }
    }

    private static Object parseLdapAttributeValue(Attribute attr) {
        final String[] values = attr.getValues();
        if (attr.getName().equals(TOKEN_GROUPS)) {
            return values.length == 1
                ? convertToString(attr.getValueByteArrays()[0])
                : Arrays.stream(attr.getValueByteArrays()).map(ActiveDirectorySIDUtil::convertToString).collect(Collectors.toList());
        }
        return values.length == 1 ? values[0] : List.of(values);

    }

    private LdapMetadataResult toLdapMetadataResult(Function<String, Attribute> attributes) {
        Attribute emailAttribute = attributes.apply(this.emailAttributeName);
        Attribute fullNameAttribute = attributes.apply(this.fullNameAttributeName);

        Map<String, Object> metaData = Arrays.stream(this.attributeNames)
            .map(attributes)
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableMap(Attribute::getName, LdapMetadataResolver::parseLdapAttributeValue));

        return new LdapMetadataResult(
            fullNameAttribute == null ? null : LdapMetadataResolver.parseLdapAttributeValue(fullNameAttribute).toString(),
            emailAttribute == null ? null : LdapMetadataResolver.parseLdapAttributeValue(emailAttribute).toString(),
            metaData
        );
    }
}
