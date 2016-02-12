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
package io.spikex.core.util;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import static java.nio.ByteOrder.BIG_ENDIAN;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.DatatypeConverter;
import net.jpountz.xxhash.XXHashFactory;

/**
 *
 * @author cli
 */
public class XXHash32 {

    private final static XXHashFactory FACTORY = XXHashFactory.fastestInstance();

    public static int hash(
            final String utf8Str,
            final int salt) {

        int hash = 0;
        try {
            byte[] data = utf8Str.getBytes("UTF-8");
            hash = FACTORY.hash32().hash(data, 0, data.length, salt);
        } catch (UnsupportedEncodingException e) {
            Logger.getLogger(XXHash32.class.getName()).log(Level.SEVERE, "", e);
        }
        return hash;
    }

    public static String hashAsHex(
            final String utf8Str,
            final int salt) {

        String hex = "";
        try {
            byte[] data = utf8Str.getBytes("UTF-8");
            int hash = FACTORY.hash32().hash(data, 0, data.length, salt);
            byte[] hashBytes = ByteBuffer.allocate(4).putInt(hash).order(BIG_ENDIAN).array();
            hex = DatatypeConverter.printHexBinary(hashBytes).toLowerCase();
        } catch (UnsupportedEncodingException e) {
            Logger.getLogger(XXHash32.class.getName()).log(Level.SEVERE, "", e);
        }
        return hex;
    }
}
