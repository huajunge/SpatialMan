package huajunge.github.io.spatialman.utils;

import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.GeometryEngine;
import huajunge.github.io.spatialman.client.Constants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Serializable;

/**
 * @author : hehuajun3
 * @description :
 * @date : Created in 2021-03-15 14:35
 * @modified by :
 **/
public class PutUtils implements Serializable {
    public static Put getPut(String oid, Geometry geometry, int location, int shape) {
        byte[] bytes = new byte[8 + oid.length()];
        ByteArrays.writeInt(location, bytes, 0);
        ByteArrays.writeInt(shape, bytes, 4);
        System.arraycopy(Bytes.toBytes(oid), 0, bytes, 8, oid.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(oid));
        String geom = GeometryEngine.geometryToWkt(geometry, 0);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(geom));
        return put;
    }

    public static Put getPut(String oid, String geometryWKT, long location, int shape) {
        byte[] bytes = new byte[8 + oid.length()];
        long indexValue = (location | ((long) shape << 32));
        //ByteArrays.writeInt(indexValue, bytes, 0);
        ByteArrays.writeLong(indexValue, bytes, 0);
        System.out.println(ByteArrays.readLong(bytes, 0));
        System.arraycopy(Bytes.toBytes(oid), 0, bytes, 8, oid.length());
        Put put = new Put(bytes);
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.O_ID), Bytes.toBytes(oid));
        put.addColumn(Bytes.toBytes(Constants.DEFAULT_CF), Bytes.toBytes(Constants.GEOM), Bytes.toBytes(geometryWKT));
        return put;
    }
}
