/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.ldap.support;

import com.unboundid.ldap.sdk.FailoverServerSet;
import com.unboundid.ldap.sdk.LDAPConnectionOptions;
import com.unboundid.ldap.sdk.RoundRobinDNSServerSet;
import com.unboundid.ldap.sdk.RoundRobinServerSet;
import com.unboundid.ldap.sdk.ServerSet;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;

import javax.net.SocketFactory;
import java.util.Arrays;
import java.util.Locale;

/**
 * Enumeration representing the various supported {@link ServerSet} types that can be used with out built in realms.
 */
public enum LdapLoadBalancing {

    FAILOVER() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            FailoverServerSet serverSet = new FailoverServerSet(addresses, ports, socketFactory, options);
            serverSet.setReOrderOnFailover(true);
            return serverSet;
        }
    },

    ROUND_ROBIN() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            return new RoundRobinServerSet(addresses, ports, socketFactory, options);
        }
    },

    DNS_ROUND_ROBIN() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            if (addresses.length != 1) {
                throw new IllegalArgumentException(toString() + " can only be used with a single url");
            }
            if (InetAddresses.isInetAddress(addresses[0])) {
                throw new IllegalArgumentException(toString() + " can only be used with a DNS name");
            }
            TimeValue dnsTtl = settings.getAsTime("cache_ttl", TimeValue.timeValueHours(1L));
            return new RoundRobinDNSServerSet(addresses[0], ports[0],
                    RoundRobinDNSServerSet.AddressSelectionMode.ROUND_ROBIN, dnsTtl.millis(), null, socketFactory, options);
        }
    },

    DNS_FAILOVER() {
        @Override
        ServerSet buildServerSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                 @Nullable LDAPConnectionOptions options) {
            if (addresses.length != 1) {
                throw new IllegalArgumentException(toString() + " can only be used with a single url");
            }
            if (InetAddresses.isInetAddress(addresses[0])) {
                throw new IllegalArgumentException(toString() + " can only be used with a DNS name");
            }
            TimeValue dnsTtl = settings.getAsTime("cache_ttl", TimeValue.timeValueHours(1L));
            return new RoundRobinDNSServerSet(addresses[0], ports[0],
                    RoundRobinDNSServerSet.AddressSelectionMode.FAILOVER, dnsTtl.millis(), null, socketFactory, options);
        }
    };

    public static final String LOAD_BALANCE_SETTINGS = "load_balance";
    public static final String LOAD_BALANCE_TYPE_SETTING = "type";
    public static final String LOAD_BALANCE_TYPE_DEFAULT = LdapLoadBalancing.FAILOVER.toString();

    abstract ServerSet buildServerSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                      @Nullable LDAPConnectionOptions options);

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    public static ServerSet serverSet(String[] addresses, int[] ports, Settings settings, @Nullable SocketFactory socketFactory,
                                      @Nullable LDAPConnectionOptions options) {
        Settings loadBalanceSettings = settings.getAsSettings(LOAD_BALANCE_SETTINGS);
        String type = loadBalanceSettings.get(LOAD_BALANCE_TYPE_SETTING, LOAD_BALANCE_TYPE_DEFAULT);
        switch (type.toLowerCase(Locale.ENGLISH)) {
            case "failover":
                return FAILOVER.buildServerSet(addresses, ports, loadBalanceSettings, socketFactory, options);
            case "dns_failover":
                return DNS_FAILOVER.buildServerSet(addresses, ports, loadBalanceSettings, socketFactory, options);
            case "round_robin":
                return ROUND_ROBIN.buildServerSet(addresses, ports, loadBalanceSettings, socketFactory, options);
            case "dns_round_robin":
                return DNS_ROUND_ROBIN.buildServerSet(addresses, ports, loadBalanceSettings, socketFactory, options);
            default:
                throw new IllegalArgumentException("unknown server set type [" + type + "]. value must be one of " +
                        Arrays.toString(LdapLoadBalancing.values()));
        }
    }
}
