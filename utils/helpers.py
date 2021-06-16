from dateutil import parser, tz

def format_created_at(created_at):
    response = parser.parse(created_at)
    zurich_tz = tz.gettz("America/São Paulo")
    obj_date = response.astimezone(zurich_tz).__str__()
    date_split = obj_date.split(' ')
    date = date_split[0].split('-')
    date_formatted = date[2] + '-' + date[1] + '-' + date[0]
    time_formatted = date_split[1].split('-')[0]
    timestamp_final = date_formatted + ' ' + time_formatted
    return timestamp_final