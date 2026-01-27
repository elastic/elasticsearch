/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.network.InetAddresses;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.ConfigurationUtils;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.Processor;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Map.entry;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.ConfigurationUtils.readBooleanProperty;

public final class CommunityIdProcessor extends AbstractProcessor {

    public static final String TYPE = "community_id";

    private static final ThreadLocal<MessageDigest> MESSAGE_DIGEST = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            // should never happen, SHA-1 must be available in all JDKs
            throw new IllegalStateException(e);
        }
    });

    private final String sourceIpField;
    private final String sourcePortField;
    private final String destinationIpField;
    private final String destinationPortField;
    private final String ianaNumberField;
    private final String transportField;
    private final String icmpTypeField;
    private final String icmpCodeField;
    private final String targetField;
    private final byte[] seed;
    private final boolean ignoreMissing;

    CommunityIdProcessor(
        String tag,
        String description,
        String sourceIpField,
        String sourcePortField,
        String destinationIpField,
        String destinationPortField,
        String ianaNumberField,
        String transportField,
        String icmpTypeField,
        String icmpCodeField,
        String targetField,
        byte[] seed,
        boolean ignoreMissing
    ) {
        super(tag, description);
        this.sourceIpField = sourceIpField;
        this.sourcePortField = sourcePortField;
        this.destinationIpField = destinationIpField;
        this.destinationPortField = destinationPortField;
        this.ianaNumberField = ianaNumberField;
        this.transportField = transportField;
        this.icmpTypeField = icmpTypeField;
        this.icmpCodeField = icmpCodeField;
        this.targetField = targetField;
        this.seed = seed;
        this.ignoreMissing = ignoreMissing;
    }

    public String getSourceIpField() {
        return sourceIpField;
    }

    public String getSourcePortField() {
        return sourcePortField;
    }

    public String getDestinationIpField() {
        return destinationIpField;
    }

    public String getDestinationPortField() {
        return destinationPortField;
    }

    public String getIanaNumberField() {
        return ianaNumberField;
    }

    public String getTransportField() {
        return transportField;
    }

    public String getIcmpTypeField() {
        return icmpTypeField;
    }

    public String getIcmpCodeField() {
        return icmpCodeField;
    }

    public String getTargetField() {
        return targetField;
    }

    public byte[] getSeed() {
        return seed;
    }

    public boolean getIgnoreMissing() {
        return ignoreMissing;
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        String sourceIp = document.getFieldValue(sourceIpField, String.class, ignoreMissing);
        String destinationIp = document.getFieldValue(destinationIpField, String.class, ignoreMissing);
        Object ianaNumber = document.getFieldValue(ianaNumberField, Object.class, true);
        Supplier<Object> transport = () -> document.getFieldValue(transportField, Object.class, ignoreMissing);
        Supplier<Object> sourcePort = () -> document.getFieldValue(sourcePortField, Object.class, ignoreMissing);
        Supplier<Object> destinationPort = () -> document.getFieldValue(destinationPortField, Object.class, ignoreMissing);
        Object icmpType = document.getFieldValue(icmpTypeField, Object.class, true);
        Object icmpCode = document.getFieldValue(icmpCodeField, Object.class, true);
        Flow flow = buildFlow(sourceIp, destinationIp, ianaNumber, transport, sourcePort, destinationPort, icmpType, icmpCode);
        if (flow == null) {
            if (ignoreMissing) {
                return document;
            } else {
                throw new IllegalArgumentException("unable to construct flow from document");
            }
        }

        document.setFieldValue(targetField, flow.toCommunityId(seed));
        return document;
    }

    public static String apply(
        String sourceIpAddrString,
        String destIpAddrString,
        Object ianaNumber,
        Object transport,
        Object sourcePort,
        Object destinationPort,
        Object icmpType,
        Object icmpCode,
        int seed
    ) {
        Flow flow = buildFlow(
            sourceIpAddrString,
            destIpAddrString,
            ianaNumber,
            () -> transport,
            () -> sourcePort,
            () -> destinationPort,
            icmpType,
            icmpCode
        );

        if (flow == null) {
            throw new IllegalArgumentException("unable to construct flow from document");
        } else {
            return flow.toCommunityId(toUint16(seed));
        }
    }

    public static String apply(
        String sourceIpAddrString,
        String destIpAddrString,
        Object ianaNumber,
        Object transport,
        Object sourcePort,
        Object destinationPort,
        Object icmpType,
        Object icmpCode
    ) {
        return apply(sourceIpAddrString, destIpAddrString, ianaNumber, transport, sourcePort, destinationPort, icmpType, icmpCode, 0);
    }

    private static Flow buildFlow(
        String sourceIpAddrString,
        String destIpAddrString,
        Object ianaNumber,
        Supplier<Object> transport,
        Supplier<Object> sourcePort,
        Supplier<Object> destinationPort,
        Object icmpType,
        Object icmpCode
    ) {
        if (sourceIpAddrString == null) {
            return null;
        }

        if (destIpAddrString == null) {
            return null;
        }

        Flow flow = new Flow();
        flow.source = InetAddresses.forString(sourceIpAddrString);
        flow.destination = InetAddresses.forString(destIpAddrString);

        Object protocol = ianaNumber;
        if (protocol == null) {
            protocol = transport.get();
            if (protocol == null) {
                return null;
            }
        }
        flow.protocol = Transport.fromObject(protocol);

        switch (flow.protocol.getType()) {
            case Tcp, Udp, Sctp -> {
                flow.sourcePort = parseIntFromObjectOrString(sourcePort.get(), "source port");
                if (flow.sourcePort < 1 || flow.sourcePort > 65535) {
                    throw new IllegalArgumentException("invalid source port [" + sourcePort.get() + "]");
                }
                flow.destinationPort = parseIntFromObjectOrString(destinationPort.get(), "destination port");
                if (flow.destinationPort < 1 || flow.destinationPort > 65535) {
                    throw new IllegalArgumentException("invalid destination port [" + destinationPort.get() + "]");
                }
            }
            case Icmp, IcmpIpV6 -> {
                // tolerate missing or invalid ICMP types and codes
                flow.icmpType = parseIntFromObjectOrString(icmpType, "icmp type");
                flow.icmpCode = parseIntFromObjectOrString(icmpCode, "icmp code");
            }
        }

        return flow;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * Converts an integer in the range of an unsigned 16-bit integer to a big-endian byte pair
     */
    // visible for testing
    static byte[] toUint16(int num) {
        if (num < 0 || num > 65535) {
            throw new IllegalStateException("number [" + num + "] must be a value between 0 and 65535");
        }
        return new byte[] { (byte) (num >> 8), (byte) num };
    }

    /**
     * Attempts to coerce an object to an integer
     */
    private static int parseIntFromObjectOrString(Object o, String fieldName) {
        if (o == null) {
            return 0;
        } else if (o instanceof Number number) {
            return number.intValue();
        } else if (o instanceof String string) {
            try {
                return Integer.parseInt(string);
            } catch (NumberFormatException e) {
                // fall through to IllegalArgumentException below
            }
        }
        throw new IllegalArgumentException("unable to parse " + fieldName + " [" + o + "]");
    }

    public static final class Factory implements Processor.Factory {

        static final String DEFAULT_SOURCE_IP = "source.ip";
        static final String DEFAULT_SOURCE_PORT = "source.port";
        static final String DEFAULT_DEST_IP = "destination.ip";
        static final String DEFAULT_DEST_PORT = "destination.port";
        static final String DEFAULT_IANA_NUMBER = "network.iana_number";
        static final String DEFAULT_TRANSPORT = "network.transport";
        static final String DEFAULT_ICMP_TYPE = "icmp.type";
        static final String DEFAULT_ICMP_CODE = "icmp.code";
        static final String DEFAULT_TARGET = "network.community_id";

        @Override
        public CommunityIdProcessor create(
            Map<String, Processor.Factory> registry,
            String tag,
            String description,
            Map<String, Object> config,
            ProjectId projectId
        ) throws Exception {
            String sourceIpField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "source_ip", DEFAULT_SOURCE_IP);
            String sourcePortField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "source_port", DEFAULT_SOURCE_PORT);
            String destIpField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "destination_ip", DEFAULT_DEST_IP);
            String destPortField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "destination_port", DEFAULT_DEST_PORT);
            String ianaNumberField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "iana_number", DEFAULT_IANA_NUMBER);
            String transportField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "transport", DEFAULT_TRANSPORT);
            String icmpTypeField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "icmp_type", DEFAULT_ICMP_TYPE);
            String icmpCodeField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "icmp_code", DEFAULT_ICMP_CODE);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "target_field", DEFAULT_TARGET);
            int seedInt = ConfigurationUtils.readIntProperty(TYPE, tag, config, "seed", 0);
            if (seedInt < 0 || seedInt > 65535) {
                throw newConfigurationException(TYPE, tag, "seed", "must be a value between 0 and 65535");
            }

            boolean ignoreMissing = readBooleanProperty(TYPE, tag, config, "ignore_missing", true);
            return new CommunityIdProcessor(
                tag,
                description,
                sourceIpField,
                sourcePortField,
                destIpField,
                destPortField,
                ianaNumberField,
                transportField,
                icmpTypeField,
                icmpCodeField,
                targetField,
                toUint16(seedInt),
                ignoreMissing
            );
        }
    }

    /**
     * Represents flow data per the <a href="https://github.com/corelight/community-id-spec">Community ID</a> spec.
     */
    private static final class Flow {

        private Flow() {
            // this is only constructable from inside this file
        }

        private static final List<Transport.Type> TRANSPORTS_WITH_PORTS = List.of(
            Transport.Type.Tcp,
            Transport.Type.Udp,
            Transport.Type.Sctp,
            Transport.Type.Icmp,
            Transport.Type.IcmpIpV6
        );

        InetAddress source;
        InetAddress destination;
        Transport protocol;
        int sourcePort;
        int destinationPort;
        int icmpType;
        int icmpCode;

        /**
         * @return true iff the source address/port is numerically less than the destination address/port as described
         * in the <a href="https://github.com/corelight/community-id-spec">Community ID</a> spec.
         */
        boolean isOrdered() {
            int result = new BigInteger(1, source.getAddress()).compareTo(new BigInteger(1, destination.getAddress()));
            return result < 0 || (result == 0 && sourcePort < destinationPort);
        }

        byte[] toBytes() {
            Transport.Type protoType = protocol.getType();
            boolean hasPort = TRANSPORTS_WITH_PORTS.contains(protoType);
            int len = source.getAddress().length + destination.getAddress().length + 2 + (hasPort ? 4 : 0);
            ByteBuffer bb = ByteBuffer.allocate(len);

            boolean isOneWay = false;
            if (protoType == Transport.Type.Icmp || protoType == Transport.Type.IcmpIpV6) {
                // ICMP protocols populate port fields with ICMP data
                Integer equivalent = IcmpType.codeEquivalent(icmpType, protoType == Transport.Type.IcmpIpV6);
                isOneWay = equivalent == null;
                sourcePort = icmpType;
                destinationPort = equivalent == null ? icmpCode : equivalent;
            }

            boolean keepOrder = isOrdered() || ((protoType == Transport.Type.Icmp || protoType == Transport.Type.IcmpIpV6) && isOneWay);
            bb.put(keepOrder ? source.getAddress() : destination.getAddress());
            bb.put(keepOrder ? destination.getAddress() : source.getAddress());
            bb.put(toUint16(protocol.getTransportNumber() << 8));

            if (hasPort) {
                bb.put(keepOrder ? toUint16(sourcePort) : toUint16(destinationPort));
                bb.put(keepOrder ? toUint16(destinationPort) : toUint16(sourcePort));
            }

            return bb.array();
        }

        String toCommunityId(byte[] seed) {
            MessageDigest md = MESSAGE_DIGEST.get();
            md.reset();
            md.update(seed);
            byte[] encodedBytes = Base64.getEncoder().encode(md.digest(toBytes()));
            return "1:" + new String(encodedBytes, StandardCharsets.UTF_8);
        }
    }

    static final class Transport {
        public enum Type {
            Unknown(-1),
            Icmp(1),
            Igmp(2),
            Tcp(6),
            Udp(17),
            Gre(47),
            IcmpIpV6(58),
            Eigrp(88),
            Ospf(89),
            Pim(103),
            Sctp(132);

            private final int transportNumber;

            private static final Map<String, Type> TRANSPORT_NAMES = Map.ofEntries(
                entry("icmp", Icmp),
                entry("igmp", Igmp),
                entry("tcp", Tcp),
                entry("udp", Udp),
                entry("gre", Gre),
                entry("ipv6-icmp", IcmpIpV6),
                entry("icmpv6", IcmpIpV6),
                entry("eigrp", Eigrp),
                entry("ospf", Ospf),
                entry("pim", Pim),
                entry("sctp", Sctp)
            );

            Type(int transportNumber) {
                this.transportNumber = transportNumber;
            }

            public int getTransportNumber() {
                return transportNumber;
            }
        }

        private final Type type;
        private final int transportNumber;

        private Transport(int transportNumber, Type type) {
            this.transportNumber = transportNumber;
            this.type = type;
        }

        private Transport(Type type) {
            this.transportNumber = type.getTransportNumber();
            this.type = type;
        }

        public Type getType() {
            return this.type;
        }

        public int getTransportNumber() {
            return transportNumber;
        }

        // visible for testing
        static Transport fromNumber(int transportNumber) {
            if (transportNumber < 0 || transportNumber >= 255) {
                // transport numbers range https://www.iana.org/assignments/protocol-numbers/protocol-numbers.xhtml
                throw new IllegalArgumentException("invalid transport protocol number [" + transportNumber + "]");
            }

            Type type = switch (transportNumber) {
                case 1 -> Type.Icmp;
                case 2 -> Type.Igmp;
                case 6 -> Type.Tcp;
                case 17 -> Type.Udp;
                case 47 -> Type.Gre;
                case 58 -> Type.IcmpIpV6;
                case 88 -> Type.Eigrp;
                case 89 -> Type.Ospf;
                case 103 -> Type.Pim;
                case 132 -> Type.Sctp;
                default -> Type.Unknown;
            };

            return new Transport(transportNumber, type);
        }

        private static Transport fromObject(Object o) {
            if (o instanceof Number number) {
                return fromNumber(number.intValue());
            } else if (o instanceof String protocolStr) {
                // check if matches protocol name
                if (Type.TRANSPORT_NAMES.containsKey(protocolStr.toLowerCase(Locale.ROOT))) {
                    return new Transport(Type.TRANSPORT_NAMES.get(protocolStr.toLowerCase(Locale.ROOT)));
                }

                // check if convertible to protocol number
                try {
                    int protocolNumber = Integer.parseInt(protocolStr);
                    return fromNumber(protocolNumber);
                } catch (NumberFormatException e) {
                    // fall through to IllegalArgumentException
                }

                throw new IllegalArgumentException("could not convert string [" + protocolStr + "] to transport protocol");
            } else {
                throw new IllegalArgumentException(
                    "could not convert value of type [" + o.getClass().getName() + "] to transport protocol"
                );
            }
        }
    }

    public enum IcmpType {
        EchoReply(0),
        EchoRequest(8),
        RouterAdvertisement(9),
        RouterSolicitation(10),
        TimestampRequest(13),
        TimestampReply(14),
        InfoRequest(15),
        InfoReply(16),
        AddressMaskRequest(17),
        AddressMaskReply(18),
        V6EchoRequest(128),
        V6EchoReply(129),
        V6RouterSolicitation(133),
        V6RouterAdvertisement(134),
        V6NeighborSolicitation(135),
        V6NeighborAdvertisement(136),
        V6MLDv1MulticastListenerQueryMessage(130),
        V6MLDv1MulticastListenerReportMessage(131),
        V6WhoAreYouRequest(139),
        V6WhoAreYouReply(140),
        V6HomeAddressDiscoveryRequest(144),
        V6HomeAddressDiscoveryResponse(145);

        private static final Map<Integer, Integer> ICMP_V4_CODE_EQUIVALENTS = Map.ofEntries(
            entry(EchoRequest.getType(), EchoReply.getType()),
            entry(EchoReply.getType(), EchoRequest.getType()),
            entry(TimestampRequest.getType(), TimestampReply.getType()),
            entry(TimestampReply.getType(), TimestampRequest.getType()),
            entry(InfoRequest.getType(), InfoReply.getType()),
            entry(RouterSolicitation.getType(), RouterAdvertisement.getType()),
            entry(RouterAdvertisement.getType(), RouterSolicitation.getType()),
            entry(AddressMaskRequest.getType(), AddressMaskReply.getType()),
            entry(AddressMaskReply.getType(), AddressMaskRequest.getType())
        );

        private static final Map<Integer, Integer> ICMP_V6_CODE_EQUIVALENTS = Map.ofEntries(
            entry(V6EchoRequest.getType(), V6EchoReply.getType()),
            entry(V6EchoReply.getType(), V6EchoRequest.getType()),
            entry(V6RouterSolicitation.getType(), V6RouterAdvertisement.getType()),
            entry(V6RouterAdvertisement.getType(), V6RouterSolicitation.getType()),
            entry(V6NeighborAdvertisement.getType(), V6NeighborSolicitation.getType()),
            entry(V6NeighborSolicitation.getType(), V6NeighborAdvertisement.getType()),
            entry(V6MLDv1MulticastListenerQueryMessage.getType(), V6MLDv1MulticastListenerReportMessage.getType()),
            entry(V6WhoAreYouRequest.getType(), V6WhoAreYouReply.getType()),
            entry(V6WhoAreYouReply.getType(), V6WhoAreYouRequest.getType()),
            entry(V6HomeAddressDiscoveryRequest.getType(), V6HomeAddressDiscoveryResponse.getType()),
            entry(V6HomeAddressDiscoveryResponse.getType(), V6HomeAddressDiscoveryRequest.getType())
        );

        private final int type;

        IcmpType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static IcmpType fromNumber(int type) {
            return switch (type) {
                case 0 -> EchoReply;
                case 8 -> EchoRequest;
                case 9 -> RouterAdvertisement;
                case 10 -> RouterSolicitation;
                case 13 -> TimestampRequest;
                case 14 -> TimestampReply;
                case 15 -> InfoRequest;
                case 16 -> InfoReply;
                case 17 -> AddressMaskRequest;
                case 18 -> AddressMaskReply;
                case 128 -> V6EchoRequest;
                case 129 -> V6EchoReply;
                case 133 -> V6RouterSolicitation;
                case 134 -> V6RouterAdvertisement;
                case 135 -> V6NeighborSolicitation;
                case 136 -> V6NeighborAdvertisement;
                case 130 -> V6MLDv1MulticastListenerQueryMessage;
                case 131 -> V6MLDv1MulticastListenerReportMessage;
                case 139 -> V6WhoAreYouRequest;
                case 140 -> V6WhoAreYouReply;
                case 144 -> V6HomeAddressDiscoveryRequest;
                case 145 -> V6HomeAddressDiscoveryResponse;
                default ->
                    // don't fail if the type is unknown
                    EchoReply;
            };
        }

        private static Integer codeEquivalent(int icmpType, boolean isIpV6) {
            return isIpV6 ? ICMP_V6_CODE_EQUIVALENTS.get(icmpType) : ICMP_V4_CODE_EQUIVALENTS.get(icmpType);
        }
    }
}
