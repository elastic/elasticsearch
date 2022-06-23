/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@SuppressWarnings("unused") // many fields are constants but required for struct layout
public final class NetworkNatives {

    private static final Logger logger = LogManager.getLogger(NetworkNatives.class);

    private static final boolean isRegistered;

    private NetworkNatives() {
        assert false; // no instances
    }

    static final short AF_NETLINK = 16;
    static final int SOCK_DGRAM = 2;
    static final int NETLINK_INET_DIAG = 4;
    static final int SOL_SOCKET = 1;
    static final int SO_RCVTIMEO = 20;
    static final byte AF_INET = 2;
    static final byte AF_INET6 = 10;
    static final byte IPPROTO_TCP = 6;
    static final int TCP_ESTABLISHED = 1;
    static final int INET_DIAG_INFO = 2;
    static final short NLM_F_DUMP = 768;
    static final short NLM_F_REQUEST = 1;
    static final short SOCK_DIAG_BY_FAMILY = 20;
    static final short NLMSG_ERROR = 2;
    static final short NLMSG_DONE = 3;

    static {
        boolean success = false;
        try {
            if (Constants.LINUX && Boolean.parseBoolean(System.getProperty("es.network_natives_enabled", "true"))) {
                try {
                    Native.register("c");
                    success = true;
                } catch (Exception e) {
                    logger.warn("native method registration failed, extended network stats will be unavailable", e);
                } catch (LinkageError e) {
                    logger.warn("unable to load JNA native support library, extended network stats will be unavailable", e);
                }
            }
        } finally {
            isRegistered = success;
        }
    }

    public static void init() {
        // just so that static constructor can be invoked
    }

    static native int socket(int domain, int type, int protocol) throws LastErrorException;

    static native int setsockopt(int fd, int level, int optname, struct_timeval optval, int optlen) throws LastErrorException;

    static native int sendmsg(int fd, struct_msghdr msg, int flags) throws LastErrorException;

    static native int recv(int fd, ByteBuffer buf, int len, int flags);

    static native int close(int fd) throws LastErrorException;

    @Structure.FieldOrder({ "tv_sec", "tv_usec" })
    public static final class struct_timeval extends Structure implements Structure.ByReference {
        public long tv_sec;
        public long tv_usec;

        public struct_timeval(long tv_sec, long tv_usec) {
            this.tv_sec = tv_sec;
            this.tv_usec = tv_usec;
        }
    }

    @Structure.FieldOrder({ "msg_name", "msg_namelen", "msg_iov", "msg_iovlen", "msg_control", "msg_controllen", "msg_flags" })
    public static final class struct_msghdr extends Structure implements Structure.ByReference {
        public struct_sockaddr_nl msg_name = new struct_sockaddr_nl(); /* optional address */
        public long msg_namelen = 12; /* size of address */
        public struct_iovec_2 msg_iov = new struct_iovec_2(); /* scatter/gather array */
        public long msg_iovlen = 2; /* # elements in msg_iov */
        public long msg_control = 0; /* ancillary data == NULL */
        public long msg_controllen = 0; /* ancillary data buffer len */
        public long msg_flags = 0; /* flags on received message */
    }

    @Structure.FieldOrder({ "nl_family", "nl_pad", "nl_pid", "nl_groups" })
    public static final class struct_sockaddr_nl extends Structure implements Structure.ByReference {
        public short nl_family = AF_NETLINK;
        public short nl_pad;
        public int nl_pid;
        public int nl_groups;
    }

    @Structure.FieldOrder({ "iov_base0", "iov_len0", "iov_base1", "iov_len1" })
    public static final class struct_iovec_2 extends Structure implements Structure.ByReference {
        public struct_nlmsghdr iov_base0 = new struct_nlmsghdr();
        public long iov_len0 = struct_nlmsghdr.LENGTH;
        public struct_inet_diag_req_v2 iov_base1 = new struct_inet_diag_req_v2();
        public long iov_len1 = struct_inet_diag_req_v2.LENGTH;
    }

    @Structure.FieldOrder({ "nlmsg_len", "nlmsg_type", "nlmsg_flags", "nlmsg_seq", "nlmsg_pid" })
    public static final class struct_nlmsghdr extends Structure implements Structure.ByReference {
        public static final int LENGTH = 16;
        public int nlmsg_len = LENGTH + struct_inet_diag_req_v2.LENGTH; /* Length of message including header */
        public short nlmsg_type = SOCK_DIAG_BY_FAMILY; /* Type of message content */
        public short nlmsg_flags = NLM_F_DUMP | NLM_F_REQUEST; /* Additional flags */
        public int nlmsg_seq; /* Sequence number */
        public int nlmsg_pid; /* Sender port ID */
    }

    @Structure.FieldOrder(
        {
            "sdiag_family",
            "sdiag_protocol",
            "idiag_ext",
            "pad",
            "idiag_states",
            "idiag_sport",
            "idiag_dport",
            "idiag_src",
            "idiag_dst",
            "idiag_if",
            "idiag_cookie" }
    )
    public static final class struct_inet_diag_req_v2 extends Structure implements Structure.ByReference {
        public static final int LENGTH = 56;
        public byte sdiag_family;
        public byte sdiag_protocol = IPPROTO_TCP;
        public byte idiag_ext = (1 << (INET_DIAG_INFO - 1));
        public byte pad;
        public int idiag_states = (1 << TCP_ESTABLISHED);
        public short idiag_sport;
        public short idiag_dport;
        public int[] idiag_src = new int[4];
        public int[] idiag_dst = new int[4];
        public int idiag_if;
        public int[] idiag_cookie = new int[2];
    }

    private static final ExtendedSocketStats[] EMPTY = new ExtendedSocketStats[0];
    private static final int RECV_BUFFER_SIZE = 8192;

    private static final ThreadLocal<ByteBuffer> recvBufferThreadLocal = ThreadLocal.withInitial(() -> {
        final ByteBuffer byteBuffer = ByteBuffer.allocateDirect(RECV_BUFFER_SIZE);
        byteBuffer.order(ByteOrder.nativeOrder());
        return byteBuffer;
    });

    public static ExtendedSocketStats[] getExtendedNetworkStats() {
        if (isRegistered == false) {
            return EMPTY;
        }

        final int fd;
        try {
            fd = socket(AF_NETLINK, SOCK_DGRAM, NETLINK_INET_DIAG);
        } catch (Exception e) {
            logger.debug("socket() failed", e);
            return EMPTY;
        }

        final List<ExtendedSocketStats> result = new ArrayList<>();

        try {
            final struct_timeval rcv_timeout = new struct_timeval(1, 0);

            final int setsockoptResult;
            try {
                setsockoptResult = setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, rcv_timeout, 16);
            } catch (Exception e) {
                logger.debug("setsockopt() failed", e);
                return EMPTY;
            }
            if (setsockoptResult != 0) {
                logger.debug("setsockopt() failed");
                return EMPTY;
            }

            for (byte family : List.of(AF_INET, AF_INET6)) {
                struct_msghdr msg = new struct_msghdr();
                msg.msg_iov.iov_base1.sdiag_family = family;

                final int bytesSent;
                try {
                    bytesSent = sendmsg(fd, msg, 0);
                } catch (Exception e) {
                    logger.debug("sendmsg() failed", e);
                    return EMPTY;
                }
                if (bytesSent != msg.msg_iov.iov_base0.nlmsg_len) {
                    logger.debug("sendmsg() sent {} bytes, expected {} bytes", bytesSent, msg.msg_iov.iov_base0.nlmsg_len);
                    return EMPTY;
                }

                final ByteBuffer recvBuffer = recvBufferThreadLocal.get();
                recvloop: while (true) {
                    recvBuffer.clear();
                    final int bytesRead;
                    try {
                        bytesRead = recv(fd, recvBuffer, RECV_BUFFER_SIZE, 0);
                    } catch (Exception e) {
                        logger.debug("recv() failed", e);
                        return EMPTY;
                    }
                    if (bytesRead == -1) {
                        logger.debug("recv() timed out");
                        return EMPTY;
                    }
                    recvBuffer.limit(bytesRead);

                    while (true) {
                        final int maxMessageLen = recvBuffer.remaining();
                        if (maxMessageLen == 0) {
                            break;
                        }

                        // each message in the buffer starts with a struct_nlmsghdr from which we read its length and type
                        if (maxMessageLen < Integer.BYTES) {
                            logger.debug("recv buffer overflow, could not get message len, {} bytes remaining", maxMessageLen);
                            return EMPTY;
                        }
                        final int messageLen = recvBuffer.getInt();
                        if (maxMessageLen < messageLen) {
                            logger.debug("recv buffer overflow, message len {} exceeds remaining bytes {}", messageLen, maxMessageLen);
                            return EMPTY;
                        }
                        if (messageLen < struct_nlmsghdr.LENGTH) {
                            logger.debug("received message length {} is shorter than message header", messageLen);
                            return EMPTY;
                        }

                        final ByteBuffer messageBuffer = recvBuffer.slice();
                        messageBuffer.order(ByteOrder.nativeOrder());
                        messageBuffer.limit(messageLen - Integer.BYTES);
                        final int alignedMessageLen = (messageLen + 3) & (~0x03);
                        if (recvBuffer.remaining() < alignedMessageLen - Integer.BYTES) {
                            logger.debug(
                                "recv buffer overflow, message len {} exceeds remaining bytes {} after alignment",
                                messageLen,
                                maxMessageLen
                            );
                            return EMPTY;
                        }
                        recvBuffer.position(recvBuffer.position() + alignedMessageLen - Integer.BYTES);

                        final short messageType = messageBuffer.getShort();
                        if (messageType == NLMSG_ERROR) {
                            logger.debug("received message type NLMSG_ERROR");
                            return EMPTY;
                        }

                        if (messageType == NLMSG_DONE) {
                            break recvloop;
                        }

                        messageBuffer.getShort(); // nlmsg_flags
                        messageBuffer.getInt(); // nlmsg_seq
                        messageBuffer.getInt(); // nlmsg_pid

                        final byte idiag_family = messageBuffer.get();
                        final byte idiag_state = messageBuffer.get();
                        final byte idiag_timer = messageBuffer.get();
                        final byte idiag_retrans = messageBuffer.get();
                        messageBuffer.order(ByteOrder.BIG_ENDIAN);
                        final short idiag_sport = messageBuffer.getShort();
                        final short idiag_dport = messageBuffer.getShort();
                        final byte[] idiag_src = new byte[16];
                        messageBuffer.get(idiag_src);
                        final byte[] idiag_dst = new byte[16];
                        messageBuffer.get(idiag_dst);
                        final InetSocketAddress src;
                        final InetSocketAddress dst;
                        try {
                            src = getInetSocketAddress(idiag_family, idiag_sport, idiag_src);
                            dst = getInetSocketAddress(idiag_family, idiag_dport, idiag_dst);
                        } catch (UnknownHostException e) {
                            // impossible
                            logger.debug("failed to parse addresses", e);
                            assert false : e;
                            return EMPTY;
                        }
                        messageBuffer.order(ByteOrder.nativeOrder());
                        final int idiag_if = messageBuffer.getInt();
                        final int[] idiag_cookie = new int[] { messageBuffer.getInt(), messageBuffer.getInt() };

                        final int idiag_expires = messageBuffer.getInt();
                        final int idiag_rqueue = messageBuffer.getInt();
                        final int idiag_wqueue = messageBuffer.getInt();
                        final int idiag_uid = messageBuffer.getInt();
                        final int idiag_inode = messageBuffer.getInt();

                        final TcpInfo tcpInfo = getTcpInfo(messageBuffer);

                        result.add(
                            new ExtendedSocketStats(
                                idiag_family,
                                idiag_state,
                                idiag_timer,
                                idiag_retrans,
                                src,
                                dst,
                                idiag_if,
                                idiag_cookie,
                                idiag_expires,
                                idiag_rqueue,
                                idiag_wqueue,
                                idiag_uid,
                                idiag_inode,
                                tcpInfo
                            )
                        );
                    }
                }
            }
        } finally {
            final int closeResult = close(fd);
            assert closeResult == 0;
        }

        return result.toArray(new ExtendedSocketStats[0]);
    }

    private static InetSocketAddress getInetSocketAddress(byte family, short port, byte[] rawAddressBytes) throws UnknownHostException {
        final byte[] addressBytes;
        if (family == AF_INET6) {
            addressBytes = rawAddressBytes;
        } else {
            addressBytes = new byte[4];
            System.arraycopy(rawAddressBytes, 0, addressBytes, 0, 4);
        }
        return new InetSocketAddress(InetAddress.getByAddress(addressBytes), port & 0xffff);
    }

    private static TcpInfo getTcpInfo(ByteBuffer buffer) {
        while (true) {
            if (buffer.remaining() < 4) {
                return null;
            }

            final int rta_len = buffer.getShort() & 0xffff;
            final int rta_type = buffer.getShort() & 0xffff;

            if (rta_type == INET_DIAG_INFO) {
                final ByteBuffer tcpinfoBuffer = buffer.slice();
                tcpinfoBuffer.limit(rta_len - 4);
                tcpinfoBuffer.order(ByteOrder.nativeOrder());

                try {
                    final byte tcpi_state = tcpinfoBuffer.get();
                    final byte tcpi_ca_state = tcpinfoBuffer.get();
                    final byte tcpi_retransmits = tcpinfoBuffer.get();
                    final byte tcpi_probes = tcpinfoBuffer.get();
                    final byte tcpi_backoff = tcpinfoBuffer.get();
                    final byte tcpi_options = tcpinfoBuffer.get();
                    final byte tcp_wscale = tcpinfoBuffer.get(); // first nibble is send window scaling, second is receive
                    tcpinfoBuffer.get(); // ints are 4-byte aligned
                    final int tcpi_rto = tcpinfoBuffer.getInt();
                    final int tcpi_ato = tcpinfoBuffer.getInt();
                    final int tcpi_snd_mss = tcpinfoBuffer.getInt();
                    final int tcpi_rcv_mss = tcpinfoBuffer.getInt();
                    final int tcpi_unacked = tcpinfoBuffer.getInt();
                    final int tcpi_sacked = tcpinfoBuffer.getInt();
                    final int tcpi_lost = tcpinfoBuffer.getInt();
                    final int tcpi_retrans = tcpinfoBuffer.getInt();
                    final int tcpi_fackets = tcpinfoBuffer.getInt();
                    final int tcpi_last_data_sent = tcpinfoBuffer.getInt();
                    final int tcpi_last_ack_sent = tcpinfoBuffer.getInt();
                    final int tcpi_last_data_recv = tcpinfoBuffer.getInt();
                    final int tcpi_last_ack_recv = tcpinfoBuffer.getInt();
                    final int tcpi_pmtu = tcpinfoBuffer.getInt();
                    final int tcpi_rcv_ssthresh = tcpinfoBuffer.getInt();
                    final int tcpi_rtt = tcpinfoBuffer.getInt();
                    final int tcpi_rttvar = tcpinfoBuffer.getInt();
                    final int tcpi_snd_ssthresh = tcpinfoBuffer.getInt();
                    final int tcpi_snd_cwnd = tcpinfoBuffer.getInt();
                    final int tcpi_advmss = tcpinfoBuffer.getInt();
                    final int tcpi_reordering = tcpinfoBuffer.getInt();
                    final int tcpi_rcv_rtt = tcpinfoBuffer.getInt();
                    final int tcpi_rcv_space = tcpinfoBuffer.getInt();
                    final int tcpi_total_retrans = tcpinfoBuffer.getInt();
                    final long tcpi_pacing_rate = tcpinfoBuffer.getLong();
                    final long tcpi_max_pacing_rate = tcpinfoBuffer.getLong();
                    final long tcpi_bytes_acked = tcpinfoBuffer.getLong();
                    final long tcpi_bytes_received = tcpinfoBuffer.getLong();
                    final int tcpi_segs_out = tcpinfoBuffer.getInt();
                    final int tcpi_segs_in = tcpinfoBuffer.getInt();
                    final int tcpi_notsent_bytes = tcpinfoBuffer.getInt();
                    final int tcpi_min_rtt = tcpinfoBuffer.getInt();
                    final int tcpi_data_segs_in = tcpinfoBuffer.getInt();
                    final int tcpi_data_segs_out = tcpinfoBuffer.getInt();
                    final long tcpi_delivery_rate = tcpinfoBuffer.getLong();
                    final long tcpi_busy_time = tcpinfoBuffer.getLong();
                    final long tcpi_rwnd_limited = tcpinfoBuffer.getLong();
                    final long tcpi_sndbuf_limited = tcpinfoBuffer.getLong();

                    final int tcpi_delivered = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;
                    final int tcpi_delivered_ce = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;
                    final long tcpi_bytes_sent = tcpinfoBuffer.remaining() > Long.BYTES ? tcpinfoBuffer.getLong() : 0;
                    final long tcpi_bytes_retrans = tcpinfoBuffer.remaining() > Long.BYTES ? tcpinfoBuffer.getLong() : 0;
                    final int tcpi_dsack_dups = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;
                    final int tcpi_reord_seen = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;
                    final int tcpi_rcv_ooopack = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;
                    final int tcpi_snd_wnd = tcpinfoBuffer.remaining() > Integer.BYTES ? tcpinfoBuffer.getInt() : 0;

                    return new TcpInfo(
                        tcpi_state,
                        tcpi_ca_state,
                        tcpi_retransmits,
                        tcpi_probes,
                        tcpi_backoff,
                        tcpi_options,
                        tcp_wscale,
                        tcpi_rto,
                        tcpi_ato,
                        tcpi_snd_mss,
                        tcpi_rcv_mss,
                        tcpi_unacked,
                        tcpi_sacked,
                        tcpi_lost,
                        tcpi_retrans,
                        tcpi_fackets,
                        tcpi_last_data_sent,
                        tcpi_last_ack_sent,
                        tcpi_last_data_recv,
                        tcpi_last_ack_recv,
                        tcpi_pmtu,
                        tcpi_rcv_ssthresh,
                        tcpi_rtt,
                        tcpi_rttvar,
                        tcpi_snd_ssthresh,
                        tcpi_snd_cwnd,
                        tcpi_advmss,
                        tcpi_reordering,
                        tcpi_rcv_rtt,
                        tcpi_rcv_space,
                        tcpi_total_retrans,
                        tcpi_pacing_rate,
                        tcpi_max_pacing_rate,
                        tcpi_bytes_acked,
                        tcpi_bytes_received,
                        tcpi_segs_out,
                        tcpi_segs_in,
                        tcpi_notsent_bytes,
                        tcpi_min_rtt,
                        tcpi_data_segs_in,
                        tcpi_data_segs_out,
                        tcpi_delivery_rate,
                        tcpi_busy_time,
                        tcpi_rwnd_limited,
                        tcpi_sndbuf_limited,
                        tcpi_delivered,
                        tcpi_delivered_ce,
                        tcpi_bytes_sent,
                        tcpi_bytes_retrans,
                        tcpi_dsack_dups,
                        tcpi_reord_seen,
                        tcpi_rcv_ooopack,
                        tcpi_snd_wnd
                    );
                } catch (BufferUnderflowException e) {
                    logger.debug("struct tcpinfo too short", e);
                    return null;
                }
            } else {
                final int alignedRemainderLength = (rta_len - 1) & (~0x03);
                if (buffer.remaining() < alignedRemainderLength) {
                    return null;
                }
                buffer.position(buffer.position() + alignedRemainderLength);
            }
        }
    }

    public static class TcpInfo {

        private final byte tcpi_state;
        private final byte tcpi_ca_state;
        private final byte tcpi_retransmits;
        private final byte tcpi_probes;
        private final byte tcpi_backoff;
        private final byte tcpi_options;
        private final byte tcp_wscale;
        private final int tcpi_rto;
        private final int tcpi_ato;
        private final int tcpi_snd_mss;
        private final int tcpi_rcv_mss;
        private final int tcpi_unacked;
        private final int tcpi_sacked;
        private final int tcpi_lost;
        private final int tcpi_retrans;
        private final int tcpi_fackets;
        private final int tcpi_last_data_sent;
        private final int tcpi_last_ack_sent;
        private final int tcpi_last_data_recv;
        private final int tcpi_last_ack_recv;
        private final int tcpi_pmtu;
        private final int tcpi_rcv_ssthresh;
        private final int tcpi_rtt;
        private final int tcpi_rttvar;
        private final int tcpi_snd_ssthresh;
        private final int tcpi_snd_cwnd;
        private final int tcpi_advmss;
        private final int tcpi_reordering;
        private final int tcpi_rcv_rtt;
        private final int tcpi_rcv_space;
        private final int tcpi_total_retrans;
        private final long tcpi_pacing_rate;
        private final long tcpi_max_pacing_rate;
        private final long tcpi_bytes_acked;
        private final long tcpi_bytes_received;
        private final int tcpi_segs_out;
        private final int tcpi_segs_in;
        private final int tcpi_notsent_bytes;
        private final int tcpi_min_rtt;
        private final int tcpi_data_segs_in;
        private final int tcpi_data_segs_out;
        private final long tcpi_delivery_rate;
        private final long tcpi_busy_time;
        private final long tcpi_rwnd_limited;
        private final long tcpi_sndbuf_limited;
        private final int tcpi_delivered;
        private final int tcpi_delivered_ce;
        private final long tcpi_bytes_sent;
        private final long tcpi_bytes_retrans;
        private final int tcpi_dsack_dups;
        private final int tcpi_reord_seen;
        private final int tcpi_rcv_ooopack;
        private final int tcpi_snd_wnd;

        public TcpInfo(
            byte tcpi_state,
            byte tcpi_ca_state,
            byte tcpi_retransmits,
            byte tcpi_probes,
            byte tcpi_backoff,
            byte tcpi_options,
            byte tcp_wscale,
            int tcpi_rto,
            int tcpi_ato,
            int tcpi_snd_mss,
            int tcpi_rcv_mss,
            int tcpi_unacked,
            int tcpi_sacked,
            int tcpi_lost,
            int tcpi_retrans,
            int tcpi_fackets,
            int tcpi_last_data_sent,
            int tcpi_last_ack_sent,
            int tcpi_last_data_recv,
            int tcpi_last_ack_recv,
            int tcpi_pmtu,
            int tcpi_rcv_ssthresh,
            int tcpi_rtt,
            int tcpi_rttvar,
            int tcpi_snd_ssthresh,
            int tcpi_snd_cwnd,
            int tcpi_advmss,
            int tcpi_reordering,
            int tcpi_rcv_rtt,
            int tcpi_rcv_space,
            int tcpi_total_retrans,
            long tcpi_pacing_rate,
            long tcpi_max_pacing_rate,
            long tcpi_bytes_acked,
            long tcpi_bytes_received,
            int tcpi_segs_out,
            int tcpi_segs_in,
            int tcpi_notsent_bytes,
            int tcpi_min_rtt,
            int tcpi_data_segs_in,
            int tcpi_data_segs_out,
            long tcpi_delivery_rate,
            long tcpi_busy_time,
            long tcpi_rwnd_limited,
            long tcpi_sndbuf_limited,
            int tcpi_delivered,
            int tcpi_delivered_ce,
            long tcpi_bytes_sent,
            long tcpi_bytes_retrans,
            int tcpi_dsack_dups,
            int tcpi_reord_seen,
            int tcpi_rcv_ooopack,
            int tcpi_snd_wnd
        ) {
            this.tcpi_state = tcpi_state;
            this.tcpi_ca_state = tcpi_ca_state;
            this.tcpi_retransmits = tcpi_retransmits;
            this.tcpi_probes = tcpi_probes;
            this.tcpi_backoff = tcpi_backoff;
            this.tcpi_options = tcpi_options;
            this.tcp_wscale = tcp_wscale;
            this.tcpi_rto = tcpi_rto;
            this.tcpi_ato = tcpi_ato;
            this.tcpi_snd_mss = tcpi_snd_mss;
            this.tcpi_rcv_mss = tcpi_rcv_mss;
            this.tcpi_unacked = tcpi_unacked;
            this.tcpi_sacked = tcpi_sacked;
            this.tcpi_lost = tcpi_lost;
            this.tcpi_retrans = tcpi_retrans;
            this.tcpi_fackets = tcpi_fackets;
            this.tcpi_last_data_sent = tcpi_last_data_sent;
            this.tcpi_last_ack_sent = tcpi_last_ack_sent;
            this.tcpi_last_data_recv = tcpi_last_data_recv;
            this.tcpi_last_ack_recv = tcpi_last_ack_recv;
            this.tcpi_pmtu = tcpi_pmtu;
            this.tcpi_rcv_ssthresh = tcpi_rcv_ssthresh;
            this.tcpi_rtt = tcpi_rtt;
            this.tcpi_rttvar = tcpi_rttvar;
            this.tcpi_snd_ssthresh = tcpi_snd_ssthresh;
            this.tcpi_snd_cwnd = tcpi_snd_cwnd;
            this.tcpi_advmss = tcpi_advmss;
            this.tcpi_reordering = tcpi_reordering;
            this.tcpi_rcv_rtt = tcpi_rcv_rtt;
            this.tcpi_rcv_space = tcpi_rcv_space;
            this.tcpi_total_retrans = tcpi_total_retrans;
            this.tcpi_pacing_rate = tcpi_pacing_rate;
            this.tcpi_max_pacing_rate = tcpi_max_pacing_rate;
            this.tcpi_bytes_acked = tcpi_bytes_acked;
            this.tcpi_bytes_received = tcpi_bytes_received;
            this.tcpi_segs_out = tcpi_segs_out;
            this.tcpi_segs_in = tcpi_segs_in;
            this.tcpi_notsent_bytes = tcpi_notsent_bytes;
            this.tcpi_min_rtt = tcpi_min_rtt;
            this.tcpi_data_segs_in = tcpi_data_segs_in;
            this.tcpi_data_segs_out = tcpi_data_segs_out;
            this.tcpi_delivery_rate = tcpi_delivery_rate;
            this.tcpi_busy_time = tcpi_busy_time;
            this.tcpi_rwnd_limited = tcpi_rwnd_limited;
            this.tcpi_sndbuf_limited = tcpi_sndbuf_limited;
            this.tcpi_delivered = tcpi_delivered;
            this.tcpi_delivered_ce = tcpi_delivered_ce;
            this.tcpi_bytes_sent = tcpi_bytes_sent;
            this.tcpi_bytes_retrans = tcpi_bytes_retrans;
            this.tcpi_dsack_dups = tcpi_dsack_dups;
            this.tcpi_reord_seen = tcpi_reord_seen;
            this.tcpi_rcv_ooopack = tcpi_rcv_ooopack;
            this.tcpi_snd_wnd = tcpi_snd_wnd;
        }

        @Override
        public String toString() {
            return "TcpInfo{"
                + "tcpi_state="
                + tcpi_state
                + ", tcpi_ca_state="
                + tcpi_ca_state
                + ", tcpi_retransmits="
                + tcpi_retransmits
                + ", tcpi_probes="
                + tcpi_probes
                + ", tcpi_backoff="
                + tcpi_backoff
                + ", tcpi_options="
                + tcpi_options
                + ", tcp_wscale="
                + tcp_wscale
                + ", tcpi_rto="
                + tcpi_rto
                + ", tcpi_ato="
                + tcpi_ato
                + ", tcpi_snd_mss="
                + tcpi_snd_mss
                + ", tcpi_rcv_mss="
                + tcpi_rcv_mss
                + ", tcpi_unacked="
                + tcpi_unacked
                + ", tcpi_sacked="
                + tcpi_sacked
                + ", tcpi_lost="
                + tcpi_lost
                + ", tcpi_retrans="
                + tcpi_retrans
                + ", tcpi_fackets="
                + tcpi_fackets
                + ", tcpi_last_data_sent="
                + tcpi_last_data_sent
                + ", tcpi_last_ack_sent="
                + tcpi_last_ack_sent
                + ", tcpi_last_data_recv="
                + tcpi_last_data_recv
                + ", tcpi_last_ack_recv="
                + tcpi_last_ack_recv
                + ", tcpi_pmtu="
                + tcpi_pmtu
                + ", tcpi_rcv_ssthresh="
                + tcpi_rcv_ssthresh
                + ", tcpi_rtt="
                + tcpi_rtt
                + ", tcpi_rttvar="
                + tcpi_rttvar
                + ", tcpi_snd_ssthresh="
                + tcpi_snd_ssthresh
                + ", tcpi_snd_cwnd="
                + tcpi_snd_cwnd
                + ", tcpi_advmss="
                + tcpi_advmss
                + ", tcpi_reordering="
                + tcpi_reordering
                + ", tcpi_rcv_rtt="
                + tcpi_rcv_rtt
                + ", tcpi_rcv_space="
                + tcpi_rcv_space
                + ", tcpi_total_retrans="
                + tcpi_total_retrans
                + ", tcpi_pacing_rate="
                + tcpi_pacing_rate
                + ", tcpi_max_pacing_rate="
                + tcpi_max_pacing_rate
                + ", tcpi_bytes_acked="
                + tcpi_bytes_acked
                + ", tcpi_bytes_received="
                + tcpi_bytes_received
                + ", tcpi_segs_out="
                + tcpi_segs_out
                + ", tcpi_segs_in="
                + tcpi_segs_in
                + ", tcpi_notsent_bytes="
                + tcpi_notsent_bytes
                + ", tcpi_min_rtt="
                + tcpi_min_rtt
                + ", tcpi_data_segs_in="
                + tcpi_data_segs_in
                + ", tcpi_data_segs_out="
                + tcpi_data_segs_out
                + ", tcpi_delivery_rate="
                + tcpi_delivery_rate
                + ", tcpi_busy_time="
                + tcpi_busy_time
                + ", tcpi_rwnd_limited="
                + tcpi_rwnd_limited
                + ", tcpi_sndbuf_limited="
                + tcpi_sndbuf_limited
                + ", tcpi_delivered="
                + tcpi_delivered
                + ", tcpi_delivered_ce="
                + tcpi_delivered_ce
                + ", tcpi_bytes_sent="
                + tcpi_bytes_sent
                + ", tcpi_bytes_retrans="
                + tcpi_bytes_retrans
                + ", tcpi_dsack_dups="
                + tcpi_dsack_dups
                + ", tcpi_reord_seen="
                + tcpi_reord_seen
                + ", tcpi_rcv_ooopack="
                + tcpi_rcv_ooopack
                + ", tcpi_snd_wnd="
                + tcpi_snd_wnd
                + '}';
        }
    }

    public static class ExtendedSocketStats {
        private final byte idiag_family;
        private final byte idiag_state;
        private final byte idiag_timer;
        private final byte idiag_retrans;
        private final InetSocketAddress idiag_src;
        private final InetSocketAddress idiag_dst;
        private final int idiag_if;
        private final int[] idiag_cookie;
        private final int idiag_expires;
        private final int idiag_rqueue;
        private final int idiag_wqueue;
        private final int idiag_uid;
        private final int idiag_inode;
        private final TcpInfo tcpInfo;

        public ExtendedSocketStats(
            byte idiag_family,
            byte idiag_state,
            byte idiag_timer,
            byte idiag_retrans,
            InetSocketAddress idiag_src,
            InetSocketAddress idiag_dst,
            int idiag_if,
            int[] idiag_cookie,
            int idiag_expires,
            int idiag_rqueue,
            int idiag_wqueue,
            int idiag_uid,
            int idiag_inode,
            TcpInfo tcpInfo
        ) {
            this.idiag_family = idiag_family;
            this.idiag_state = idiag_state;
            this.idiag_timer = idiag_timer;
            this.idiag_retrans = idiag_retrans;
            this.idiag_src = idiag_src;
            this.idiag_dst = idiag_dst;
            this.idiag_if = idiag_if;
            this.idiag_cookie = idiag_cookie;
            this.idiag_expires = idiag_expires;
            this.idiag_rqueue = idiag_rqueue;
            this.idiag_wqueue = idiag_wqueue;
            this.idiag_uid = idiag_uid;
            this.idiag_inode = idiag_inode;
            this.tcpInfo = tcpInfo;
        }

        @Override
        public String toString() {
            return "ExtendedSocketStats{"
                + "idiag_family="
                + idiag_family
                + ", idiag_state="
                + idiag_state
                + ", idiag_timer="
                + idiag_timer
                + ", idiag_retrans="
                + idiag_retrans
                + ", idiag_src="
                + NetworkAddress.format(idiag_src)
                + ", idiag_dst="
                + NetworkAddress.format(idiag_dst)
                + ", idiag_if="
                + idiag_if
                + ", idiag_cookie="
                + Arrays.toString(idiag_cookie)
                + ", idiag_expires="
                + idiag_expires
                + ", idiag_rqueue="
                + idiag_rqueue
                + ", idiag_wqueue="
                + idiag_wqueue
                + ", idiag_uid="
                + idiag_uid
                + ", idiag_inode="
                + idiag_inode
                + ", tcpInfo="
                + tcpInfo
                + '}';
        }
    }

}
