/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.ldap.support;

import com.unboundid.ldap.sdk.FailoverServerSet;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.RoundRobinDNSServerSet;
import com.unboundid.ldap.sdk.RoundRobinServerSet;
import com.unboundid.ldap.sdk.ServerSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.support.LdapLoadBalancingSettings;

import javax.net.SocketFactory;
import java.util.Locale;

/**
 * Enumeration representing the various supported {@link ServerSet} types that can be used with out built in realms.
 */
public enum LdapLoadBalancing {

    FAILOVER() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            FailoverServerSet serverSet = new FailoverServerSet(addresses, ports, socketFactory, options);
            serverSet.setReOrderOnFailover(true);
            return serverSet;
        }
    },

    ROUND_ROBIN() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            return new RoundRobinServerSet(addresses, ports, socketFactory, options);
        }
    },

    DNS_ROUND_ROBIN() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            if (addresses.length != 1) {
                throw new IllegalArgumentException(toString() + " can only be used with a single url");
            }
            if (InetAddresses.isInetAddress(addresses[0])) {
                throw new IllegalArgumentException(toString() + " can only be used with a DNS name");
            }
            TimeValue dnsTtl = realmConfig.getSetting(LdapLoadBalancingSettings.CACHE_TTL_SETTING);
            return new RoundRobinDNSServerSet(addresses[0], ports[0],
                    RoundRobinDNSServerSet.AddressSelectionMode.ROUND_ROBIN, dnsTtl.millis(), null, socketFactory, options);
        }
    },

    DNS_FAILOVER() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            if (addresses.length != 1) {
                throw new IllegalArgumentException(toString() + " can only be used with a single url");
            }
            if (InetAddresses.isInetAddress(addresses[0])) {
                throw new IllegalArgumentException(toString() + " can only be used with a DNS name");
            }
            TimeValue dnsTtl = realmConfig.getSetting(LdapLoadBalancingSettings.CACHE_TTL_SETTING);
            return new RoundRobinDNSServerSet(addresses[0], ports[0],
                    RoundRobinDNSServerSet.AddressSelectionMode.FAILOVER, dnsTtl.millis(), null, socketFactory, options);
        }
    };

    public static final LdapLoadBalancing LOAD_BALANCE_TYPE_DEFAULT = LdapLoadBalancing.FAILOVER;

    abstract ServerSet buildServerSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                      @Nullable LDAPConnectionOptions options);

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static LdapLoadBalancing resolve(RealmConfig realmConfig) {
        String type = realmConfig.getSetting(LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING);
        if (Strings.isNullOrEmpty(type)) {
            return LOAD_BALANCE_TYPE_DEFAULT;
        }
        try {
            return valueOf(type.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException ilae) {
            throw new IllegalArgumentException("unknown load balance type [" + type + "] in setting [" +
                    RealmSettings.getFullSettingKey(realmConfig, LdapLoadBalancingSettings.LOAD_BALANCE_TYPE_SETTING) +
                    "]", ilae);
        }
    }

    public static ServerSet serverSet(String[] addresses, int[] ports, RealmConfig realmConfig, @Nullable SocketFactory socketFactory,
                                      @Nullable LDAPConnectionOptions options) {
        LdapLoadBalancing loadBalancing = resolve(realmConfig);
        return loadBalancing.buildServerSet(addresses, ports, realmConfig, socketFactory, options);
    }

}
