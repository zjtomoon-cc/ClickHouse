#pragma once


#include <Interpreters/SystemLog.h>
#include <Core/NamesAndTypes.h>
#include <Core/NamesAndAliases.h>
#include <Poco/Message.h>

namespace DB
{

struct BlobStorageLogElement
{
    enum class EventType : Int8
    {
        Upload = 1,
        Delete = 2,
        MultiPartUploadCreate = 3,
        MultiPartUploadWrite = 4,
        MultiPartUploadComplete = 5,
        MultiPartUploadAbort = 6,
    };

    time_t event_time = 0;
    Decimal64 event_time_microseconds = 0;
    EventType event_type;

    String disk_name;
    String bucket;
    String remote_path;

    String referring_local_path;

    BlobStorageLogElement() = default;

    static std::string name() { return "BlobStorageLog"; }

    static NamesAndTypesList getNamesAndTypes();
    static NamesAndAliases getNamesAndAliases() { return {}; }
    void appendToBlock(MutableColumns & columns) const;
    static const char * getCustomColumnList() { return nullptr; }
};


class BlobStorageLog : public SystemLog<BlobStorageLogElement>
{
    using SystemLog<BlobStorageLogElement>::SystemLog;
};

/// Writes events to BlobStorageLog
/// May contains some context information
class BlobStorageLogWriter
{
    std::shared_ptr<BlobStorageLog> log;

    String disk_name;
    String referring_local_path;

public:
    BlobStorageLogWriter() = default;

    explicit BlobStorageLogWriter(std::shared_ptr<BlobStorageLog> log_, const String & disk_name_ = "", const String & referring_local_path_ = "")
        : log(std::move(log_)), disk_name(disk_name_), referring_local_path(referring_local_path_)
    {}

    void addEvent(
        BlobStorageLogElement::EventType event_type,
        const String & bucket,
        const String & remote_path,
        const String & local_path = "");
};

}
