#include <Interpreters/S3BlobLog.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDate.h>

namespace DB
{

NamesAndTypesList S3BlobLogElement::getNamesAndTypes()
{
    return {
        {
            "event_type",
            std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values
            {
                {"Upload", static_cast<Int8>(EventType::Upload)},
                {"Delete", static_cast<Int8>(EventType::Delete)},
                {"MultiPartUploadCreate", static_cast<Int8>(EventType::MultiPartUploadCreate)},
                {"MultiPartUploadWrite", static_cast<Int8>(EventType::MultiPartUploadWrite)},
                {"MultiPartUploadComplete", static_cast<Int8>(EventType::MultiPartUploadComplete)},
                {"MultiPartUploadAbort", static_cast<Int8>(EventType::MultiPartUploadAbort)},
            })
        },
        {"disk_name", std::make_shared<DataTypeString>()},
        {"bucket", std::make_shared<DataTypeString>()},
        {"remote_path", std::make_shared<DataTypeString>()},
        {"referring_local_path", std::make_shared<DataTypeString>()},

        {"event_date", std::make_shared<DataTypeDate>()},
        {"event_time", std::make_shared<DataTypeDateTime>()},
        {"event_time_microseconds", std::make_shared<DataTypeDateTime64>(6)},
    };
}


void S3BlobLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t i = 0;

    columns[i++]->insert(static_cast<Int8>(event_type));

    columns[i++]->insert(disk_name);
    columns[i++]->insert(bucket);
    columns[i++]->insert(remote_path);

    columns[i++]->insert(referring_local_path);

    columns[i++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[i++]->insert(event_time);
    columns[i++]->insert(event_time_microseconds);

    assert([&]()
    {
        size_t total_colums = S3BlobLogElement::getNamesAndTypes().size();
        return i == total_colums;
    }());
}

void S3BlobLogWriter::addEvent(S3BlobLogElement::EventType event_type, const String & bucket, const String & remote_path, const String & local_path)
{
    if (!log)
        return;

    S3BlobLogElement element;
    element.event_type = event_type;

    element.bucket = bucket;
    element.remote_path = remote_path;
    element.referring_local_path = local_path.empty() ? referring_local_path : local_path;

    element.disk_name = disk_name;

    const auto time_now = std::chrono::system_clock::now();
    element.event_time = timeInSeconds(time_now);
    element.event_time_microseconds = timeInMicroseconds(time_now);

    log->add(element);
}

}
