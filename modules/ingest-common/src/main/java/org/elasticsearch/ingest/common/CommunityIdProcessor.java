/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

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
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;

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
    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
        String sourceIp = ingestDocument.getFieldValue(sourceIpField, String.class, ignoreMissing);
        String destinationIp = ingestDocument.getFieldValue(destinationIpField, String.class, ignoreMissing);
        Object ianaNumber = ingestDocument.getFieldValue(ianaNumberField, Object.class, true);
        Supplier<Object> transport = () -> ingestDocument.getFieldValue(transportField, Object.class, ignoreMissing);
        Supplier<Object> sourcePort = () -> ingestDocument.getFieldValue(sourcePortField, Object.class, ignoreMissing);
        Supplier<Object> destinationPort = () -> ingestDocument.getFieldValue(destinationPortField, Object.class, ignoreMissing);
        Object icmpType = ingestDocument.getFieldValue(icmpTypeField, Object.class, true);
        Object icmpCode = ingestDocument.getFieldValue(icmpCodeField, Object.class, true);
        Flow flow = buildFlow(sourceIp, destinationIp, ianaNumber, transport, sourcePort, destinationPort, icmpType, icmpCode);
        if (flow == null) {
            if (ignoreMissing) {
                return ingestDocument;
            } else {
                throw new IllegalArgumentException("unable to construct flow from document");
            }
        }

        ingestDocument.setFieldValue(targetField, flow.toCommunityId(seed));
        return ingestDocument;
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
        int seed) {

        Flow flow = buildFlow(sourceIpAddrString, destIpAddrString, ianaNumber, () -> transport, () -> sourcePort, () -> destinationPort,
            icmpType, icmpCode);

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
        Object icmpCode) {
        return apply(sourceIpAddrString, destIpAddrString, ianaNumber, transport, sourcePort, destinationPort, icmpType, icmpCode, 0);
    }

    private static Flow buildFlow(String sourceIpAddrString, String destIpAddrString, Object ianaNumber,
        Supplier<Object> transport, Supplier<Object> sourcePort, Supplier<Object> destinationPort,
        Object icmpType, Object icmpCode) {
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

        switch (flow.protocol) {
            case Tcp:
            case Udp:
            case Sctp:
                flow.sourcePort = parseIntFromObjectOrString(sourcePort.get(), "source port");
                if (flow.sourcePort < 1 || flow.sourcePort > 65535) {
                    throw new IllegalArgumentException("invalid source port [" + sourcePort.get() + "]");
                }

                flow.destinationPort = parseIntFromObjectOrString(destinationPort.get(), "destination port");
                if (flow.destinationPort < 1 || flow.destinationPort > 65535) {
                    throw new IllegalArgumentException("invalid destination port [" + destinationPort.get() + "]");
                }
                break;
            case Icmp:
            case IcmpIpV6:
                // tolerate missing or invalid ICMP types and codes
                flow.icmpType = parseIntFromObjectOrString(icmpType, "icmp type");
                flow.icmpCode = parseIntFromObjectOrString(icmpCode, "icmp code");
                break;
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
    static byte[] toUint16(int num) {
        if (num < 0 || num > 65535) {
            throw new IllegalStateException("number [" + num + "] must be a value between 0 and 65535");
        }
        return new byte[] { (byte) (num >> 8), (byte) num };
    }

    /**
     * Attempts to coerce an object to an integer
     */
    static int parseIntFromObjectOrString(Object o, String fieldName) {
        if (o == null) {
            return 0;
        } else if (o instanceof Number) {
            return ((Number) o).intValue();
        } else if (o instanceof String) {
            try {
                return Integer.parseInt((String) o);
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
            String processorTag,
            String description,
            Map<String, Object> config
        ) throws Exception {
            String sourceIpField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "source_ip", DEFAULT_SOURCE_IP);
            String sourcePortField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "source_port", DEFAULT_SOURCE_PORT);
            String destIpField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "destination_ip", DEFAULT_DEST_IP);
            String destPortField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "destination_port", DEFAULT_DEST_PORT);
            String ianaNumberField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "iana_number", DEFAULT_IANA_NUMBER);
            String transportField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "transport", DEFAULT_TRANSPORT);
            String icmpTypeField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "icmp_type", DEFAULT_ICMP_TYPE);
            String icmpCodeField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "icmp_code", DEFAULT_ICMP_CODE);
            String targetField = ConfigurationUtils.readStringProperty(TYPE, processorTag, config, "target_field", DEFAULT_TARGET);
            int seedInt = ConfigurationUtils.readIntProperty(TYPE, processorTag, config, "seed", 0);
            if (seedInt < 0 || seedInt > 65535) {
                throw newConfigurationException(TYPE, processorTag, "seed", "must be a value between 0 and 65535");
            }

            boolean ignoreMissing = readBooleanProperty(TYPE, processorTag, config, "ignore_missing", true);
            return new CommunityIdProcessor(
                processorTag,
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
     * Represents flow data per https://github.com/corelight/community-id-spec
     */
    public static final class Flow {

        private static final List<Transport> TRANSPORTS_WITH_PORTS = List.of(
            Transport.Tcp,
            Transport.Udp,
            Transport.Sctp,
            Transport.Icmp,
            Transport.IcmpIpV6
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
         * at https://github.com/corelight/community-id-spec
         */
        boolean isOrdered() {
            int result = new BigInteger(1, source.getAddress()).compareTo(new BigInteger(1, destination.getAddress()));
            return result < 0 || (result == 0 && sourcePort < destinationPort);
        }

        byte[] toBytes() {
            boolean hasPort = TRANSPORTS_WITH_PORTS.contains(protocol);
            int len = source.getAddress().length + destination.getAddress().length + 2 + (hasPort ? 4 : 0);
            ByteBuffer bb = ByteBuffer.allocate(len);

            boolean isOneWay = false;
            if (protocol == Transport.Icmp || protocol == Transport.IcmpIpV6) {
                // ICMP protocols populate port fields with ICMP data
                Integer equivalent = IcmpType.codeEquivalent(icmpType, protocol == Transport.IcmpIpV6);
                isOneWay = equivalent == null;
                sourcePort = icmpType;
                destinationPort = equivalent == null ? icmpCode : equivalent;
            }

            boolean keepOrder = isOrdered() || ((protocol == Transport.Icmp || protocol == Transport.IcmpIpV6) && isOneWay);
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

    public enum Transport {
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

        private static final Map<String, Transport> TRANSPORT_NAMES;

        static {
            TRANSPORT_NAMES = new HashMap<>();
            TRANSPORT_NAMES.put("icmp", Icmp);
            TRANSPORT_NAMES.put("igmp", Igmp);
            TRANSPORT_NAMES.put("tcp", Tcp);
            TRANSPORT_NAMES.put("udp", Udp);
            TRANSPORT_NAMES.put("gre", Gre);
            TRANSPORT_NAMES.put("ipv6-icmp", IcmpIpV6);
            TRANSPORT_NAMES.put("icmpv6", IcmpIpV6);
            TRANSPORT_NAMES.put("eigrp", Eigrp);
            TRANSPORT_NAMES.put("ospf", Ospf);
            TRANSPORT_NAMES.put("pim", Pim);
            TRANSPORT_NAMES.put("sctp", Sctp);
        }

        Transport(int transportNumber) {
            this.transportNumber = transportNumber;
        }

        public int getTransportNumber() {
            return transportNumber;
        }

        public static Transport fromNumber(int transportNumber) {
            switch (transportNumber) {
                case 1:
                    return Icmp;
                case 2:
                    return Igmp;
                case 6:
                    return Tcp;
                case 17:
                    return Udp;
                case 47:
                    return Gre;
                case 58:
                    return IcmpIpV6;
                case 88:
                    return Eigrp;
                case 89:
                    return Ospf;
                case 103:
                    return Pim;
                case 132:
                    return Sctp;
                default:
                    throw new IllegalArgumentException("unknown transport protocol number [" + transportNumber + "]");
            }
        }

        public static Transport fromObject(Object o) {
            if (o instanceof Number) {
                return fromNumber(((Number) o).intValue());
            } else if (o instanceof String) {
                String protocolStr = (String) o;

                // check if matches protocol name
                if (TRANSPORT_NAMES.containsKey(protocolStr.toLowerCase(Locale.ROOT))) {
                    return TRANSPORT_NAMES.get(protocolStr.toLowerCase(Locale.ROOT));
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

        private static final Map<Integer, Integer> ICMP_V4_CODE_EQUIVALENTS;
        private static final Map<Integer, Integer> ICMP_V6_CODE_EQUIVALENTS;

        static {
            ICMP_V4_CODE_EQUIVALENTS = new HashMap<>();
            ICMP_V4_CODE_EQUIVALENTS.put(EchoRequest.getType(), EchoReply.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(EchoReply.getType(), EchoRequest.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(TimestampRequest.getType(), TimestampReply.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(TimestampReply.getType(), TimestampRequest.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(InfoRequest.getType(), InfoReply.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(RouterSolicitation.getType(), RouterAdvertisement.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(RouterAdvertisement.getType(), RouterSolicitation.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(AddressMaskRequest.getType(), AddressMaskReply.getType());
            ICMP_V4_CODE_EQUIVALENTS.put(AddressMaskReply.getType(), AddressMaskRequest.getType());

            ICMP_V6_CODE_EQUIVALENTS = new HashMap<>();
            ICMP_V6_CODE_EQUIVALENTS.put(V6EchoRequest.getType(), V6EchoReply.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6EchoReply.getType(), V6EchoRequest.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6RouterSolicitation.getType(), V6RouterAdvertisement.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6RouterAdvertisement.getType(), V6RouterSolicitation.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6NeighborAdvertisement.getType(), V6NeighborSolicitation.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6NeighborSolicitation.getType(), V6NeighborAdvertisement.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6MLDv1MulticastListenerQueryMessage.getType(), V6MLDv1MulticastListenerReportMessage.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6WhoAreYouRequest.getType(), V6WhoAreYouReply.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6WhoAreYouReply.getType(), V6WhoAreYouRequest.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6HomeAddressDiscoveryRequest.getType(), V6HomeAddressDiscoveryResponse.getType());
            ICMP_V6_CODE_EQUIVALENTS.put(V6HomeAddressDiscoveryResponse.getType(), V6HomeAddressDiscoveryRequest.getType());
        }

        private final int type;

        IcmpType(int type) {
            this.type = type;
        }

        public int getType() {
            return type;
        }

        public static IcmpType fromNumber(int type) {
            switch (type) {
                case 0:
                    return EchoReply;
                case 8:
                    return EchoRequest;
                case 9:
                    return RouterAdvertisement;
                case 10:
                    return RouterSolicitation;
                case 13:
                    return TimestampRequest;
                case 14:
                    return TimestampReply;
                case 15:
                    return InfoRequest;
                case 16:
                    return InfoReply;
                case 17:
                    return AddressMaskRequest;
                case 18:
                    return AddressMaskReply;
                case 128:
                    return V6EchoRequest;
                case 129:
                    return V6EchoReply;
                case 133:
                    return V6RouterSolicitation;
                case 134:
                    return V6RouterAdvertisement;
                case 135:
                    return V6NeighborSolicitation;
                case 136:
                    return V6NeighborAdvertisement;
                case 130:
                    return V6MLDv1MulticastListenerQueryMessage;
                case 131:
                    return V6MLDv1MulticastListenerReportMessage;
                case 139:
                    return V6WhoAreYouRequest;
                case 140:
                    return V6WhoAreYouReply;
                case 144:
                    return V6HomeAddressDiscoveryRequest;
                case 145:
                    return V6HomeAddressDiscoveryResponse;
                default:
                    // don't fail if the type is unknown
                    return EchoReply;
            }
        }

        public static Integer codeEquivalent(int icmpType, boolean isIpV6) {
            return isIpV6 ? ICMP_V6_CODE_EQUIVALENTS.get(icmpType) : ICMP_V4_CODE_EQUIVALENTS.get(icmpType);
        }
    }
}
