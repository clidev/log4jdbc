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
package io.spikex.test;

import io.spikex.core.util.XXHash32;
import java.io.IOException;
import net.sf.log4jdbc.SimpleCollectdClient;
import org.junit.Test;

/**
 *
 * @author cli
 */
public final class SimpleCollectdClientTest {

    private static final String COLLECTD_PLUGIN = "sql";
    private static final String COLLECTD_TYPE = "duration_ms";

    private static final String SQL_TEST = "SELECT * FROM mytable";
    private static final int XXHash32_SALT = 0x734143;

    @Test
    public void testPackets() throws IOException {
        SimpleCollectdClient client = new SimpleCollectdClient(COLLECTD_PLUGIN, COLLECTD_TYPE);
        client.init();
        for (int i = 0; i < 1000; i++) {
            String hash = XXHash32.hashAsHex(SQL_TEST, XXHash32_SALT);
            long duration = (long) (Math.random() * 100.0d);
            client.sendPacket("test", hash, duration, SQL_TEST);
        }
    }
}
