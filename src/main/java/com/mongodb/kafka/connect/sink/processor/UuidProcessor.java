package com.mongodb.kafka.connect.sink.processor;

import com.mongodb.kafka.connect.sink.MongoSinkTopicConfig;
import com.mongodb.kafka.connect.sink.converter.SinkDocument;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.*;

import java.util.UUID;

public class UuidProcessor extends PostProcessor {

    private static final String CONFIG_KEY = "uuid.processor.keys";
    private final String[] transformKeys;

    public UuidProcessor(final MongoSinkTopicConfig config) {
        super(config);
        transformKeys = config.getString(CONFIG_KEY).split(",");
    }

    @Override
    public void process(final SinkDocument doc, final SinkRecord orig) {
        doc.getValueDoc().ifPresent(vd -> {
            for (String transformKey : transformKeys) {
                final BsonValue bsonValue = vd.remove(transformKey);

                if (bsonValue instanceof BsonString) {
                    vd.put(transformKey, transformBsonStringToBinary((BsonString) bsonValue));
                }
            }
        });
    }

    private BsonBinary transformBsonStringToBinary(BsonString bsonString) {
        return new BsonBinary(
                UUID.fromString(bsonString.getValue()),
                UuidRepresentation.STANDARD
        );
    }
}
