package org.apache.hadoop.hive.serde2.lazy;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Text;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * TestLazyStruct.<br/>
 */
public class TestLazyStruct {

    @Test
    public void testParseMultiDelimit() throws Throwable {
        try {
            // single column named id
            List<String> columns = new ArrayList<>();
            columns.add("id");
            // column type is string
            List<TypeInfo> columnTypes = new ArrayList<>();
            PrimitiveTypeInfo primitiveTypeInfo = new PrimitiveTypeInfo();
            primitiveTypeInfo.setTypeName("string");
            columnTypes.add(primitiveTypeInfo);

            // separators + escapeChar => "|"
            byte[] separators = new byte[]{124, 2, 3, 4, 5, 6, 7, 8};

            // sequence =>"\N"
            Text sequence = new Text();
            sequence.set(new byte[]{92, 78});

            // create a lazy struct inspector
            ObjectInspector objectInspector = LazyFactory.createLazyStructInspector(columns, columnTypes, separators,
                    sequence, false, false, (byte) '0');
            LazyStruct lazyStruct = (LazyStruct) LazyFactory.createLazyObject(objectInspector);

            // origin row data
            String rowData = "1|@|";
            // row field delimiter
            String fieldDelimiter = "|@|";

            // parse row use multi delimit
            lazyStruct.parseMultiDelimit(rowData.getBytes(StandardCharsets.UTF_8),
                    fieldDelimiter.getBytes(StandardCharsets.UTF_8));

            // check the first field and second field start position index
            // before fix result: 0,1
            // after fix result: 0,2
            Assert.assertArrayEquals(new int[]{0, 2}, lazyStruct.startPosition);
        } catch (Throwable e) {
            e.printStackTrace();
            throw e;
        }

    }
}
