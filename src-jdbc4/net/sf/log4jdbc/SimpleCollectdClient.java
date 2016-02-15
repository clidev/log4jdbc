/**
 *
 * Copyright (c) 2016 NG Modular Oy.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package net.sf.log4jdbc;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.DatagramChannel;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cli
 */
public final class SimpleCollectdClient {

    private final String m_plugin; // Spike.x default mapping: ${plugin}${sep}${type}
    //private final String m_pluginInstance; // Spike.x instance
    private final String m_type; // Spike.x default mapping: ${plugin}${sep}${type}
    //private final String m_typeInstance; // Spikex. subgroup

    private String m_hostname;
    private DatagramChannel m_channel;
    private InetSocketAddress m_socketAddress;
    private ByteBuffer m_buf;
    private ByteBuffer m_valueBuf;

    private static final int HEADER_LEN = 4;

    public SimpleCollectdClient(
            final String plugin,
            final String type) {

        m_plugin = plugin;
        m_type = type;
    }

    public void init() {
        try {
            m_hostname = java.net.InetAddress.getLocalHost().getHostName();
            String address = System.getProperty("net.sf.log4jdbc.spikex.address", "localhost");
            String port = System.getProperty("net.sf.log4jdbc.spikex.port", "25826");
            LoggerFactory.getLogger("collectd").info("Collectd host: {} port: {}", address, port);
            m_socketAddress = new InetSocketAddress(address, Integer.parseInt(port));
            m_channel = DatagramChannel.open();
            m_buf = ByteBuffer.allocate(1024); // Big-endian by default
            m_valueBuf = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN); // Little-endian
        } catch (IOException e) {
            LoggerFactory.getLogger("collectd").error("Failed to initialize UDP client", e);
        }
    }

    public void sendPacket(
            final String subgroup,
            final String sqlHash,
            final long execMs,
            final String sql) {

        try {
            // Hostname
            ByteBuffer buf = m_buf;
            buf.clear();
            appendString(buf, (short) 0x0000, m_hostname);

            // Timestamp (highres)
            appendLong(buf, (short) 0x0008, (System.currentTimeMillis() / 1000L) * 1073741824L);

            // Plugin
            appendString(buf, (short) 0x0002, m_plugin);

            // Plugin instance (Spike.x instance)
            // hash only if sql is empty
            if (sql == null || sql.length() == 0) {
                appendString(buf, (short) 0x0003, sqlHash);
            } else {
                // hash + small part of the SQL
                StringBuilder sb = new StringBuilder(sqlHash);
                sb.append(":");
                // Translate special chars
                String sqlStr = sql.replace("'", " ");
                if (sqlStr.length() > 100) {
                    sb.append(sqlStr.substring(0, 100));
                } else {
                    sb.append(sqlStr);
                }
                appendString(buf, (short) 0x0003, sb.toString());
            }

            // Type
            appendString(buf, (short) 0x0004, m_type);

            // Type instance (Spike.x subgroup)
            appendString(buf, (short) 0x0005, subgroup);

            // Values
            appendValue(buf, (short) 0x0006, execMs);

            // Interval
            appendLong(buf, (short) 0x0007, 10L * 1073741824L);

            // Bombs away...
            buf.flip();
            m_channel.send(buf, m_socketAddress);

        } catch (Exception e) {
            LoggerFactory.getLogger("collectd").error("Failed to send UDP packet", e);
        }
    }

    private void appendString(
            final ByteBuffer buf,
            final short partId,
            final String str) throws UnsupportedEncodingException {

        // Sanity check
        if (str == null || str.length() == 0) {
            return;
        }

        // Convert string to US-ASCII bytes
        byte[] data = str.getBytes("US-ASCII");

        // Header
        // 0-1: Type
        // 2-3: Length: header + payload
        // 4-n: String
        // n+1: '\0'
        buf.putShort(partId);
        buf.putShort((short) (HEADER_LEN + data.length + 1));

        // Payload
        buf.put(data);
        buf.put((byte) 0); // End-of-string
    }

    private void appendLong(
            final ByteBuffer buf,
            final short partId,
            final long value) {

        // Header
        // 0-1: Type
        // 2-3: Length: header + payload
        // 4-11: Value
        buf.putShort(partId);
        buf.putShort((short) (HEADER_LEN + 8));

        // Payload
        buf.putLong(value);
    }

    private void appendValue(
            final ByteBuffer buf,
            final short partId,
            final long value) {

        // Header
        // 0-1: Type
        // 2-3: Length: header + payload
        // 4-5: Number of values
        // 6-n: Types and values
        buf.putShort(partId);
        buf.putShort((short) (HEADER_LEN + 2 + 1 + 8));
        buf.putShort((short) 1);

        // Type
        buf.put((byte) 1); // GAUGE

        // Value (GAUGE is in Little-Endian)
        ByteBuffer valueBuf = m_valueBuf;
        valueBuf.clear();
        valueBuf.putDouble(value);
        valueBuf.flip();
        buf.put(valueBuf);
    }
}
