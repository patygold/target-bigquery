import json

import singer

from target_bigquery.processhandler import BaseProcessHandler

logger = singer.get_logger()


def process(
        ProcessHandler,
        tap_stream,
        **kwargs
):
    """
    For every line in tap_stream:
        - parses JSON
        - determines whether it's state, record or schema,
            and handles it accordingly

    :param ProcessHandler: can be LoadJobProcessHandler, PartialLoadJobProcessHandler
        or BookmarksStatePartialLoadJobProcessHandler
    :param tap_stream, TextIOWrapper. tap_stream.name has a full file path as a string.
    :param kwargs: dict. Contains parameters such as:
            - initial_state
            - project_id
            - dataset
            - location
            - truncate
            - validate_records
            - allow_field_addition
            ...

    :return: state, State
    """
    handler = ProcessHandler(logger, **kwargs)
    assert isinstance(handler, BaseProcessHandler)

    if handler.emit_initial_state():
        s = kwargs.get("initial_state", None)
        if isinstance(s, dict):
            logger.info(f"Pushing state: {s}")
            yield s  # yield init state, so even if there is an exception right after, we get proper state emitted

    for line in tap_stream:
        # parse JSON and extract SchemaMessage
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        # determine whether this line is state, record or schema and handle it accordingly
        if isinstance(msg, singer.RecordMessage):
            for s in handler.handle_record_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.StateMessage):
            logger.info("Updating state with {}".format(msg.value))
            for s in handler.handle_state_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.SchemaMessage):
            logger.info("{} schema: {}".format(msg.stream, msg.schema))
            for s in handler.handle_schema_message(msg):
                logger.info(f"Pushing state: {s}")
                yield s

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for s in handler.on_stream_end():
        logger.info(f"Pushing state: {s}")
        yield s
