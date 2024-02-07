import csv
import requests
import pandas as pd
import matplotlib.pyplot as plt


def csv_grabber():
    """ asks and validates the csv_file location

    :return: string for the csv_file location name
    """

    while True:
        try:
            file_location = input("File location for the 3-day solar panel records (file should end with .csv): ")

            with open(file_location, "r") as solar_data:
                pass

            break
        except FileNotFoundError:
            print(f"Invalid input. {file_location} does not exist.\n")

    return file_location


def API_grabber(file_location):
    """ utilizes the Visual Crossing Weather API through RapidAPI to provide weather data on selected dates

    :param file_location: the location of the csv file of solar data
    :return: a json file filled with everything weather related, very messy and needs to be queried
    """

    # File I/O to read csv file
    csv_file = open(file_location, 'r')
    csv_reader = list(csv.reader(csv_file))
    csv_file.close()

    # Formats the dates/hours for API
    formatted_start_date = csv_reader[1][0][:10]
    formatted_end_date = csv_reader[-1][0][:10]
    formatted_end_hour = csv_reader[-1][0][-8:-6]

    url = "https://visual-crossing-weather.p.rapidapi.com/history"

    querystring = {"startDateTime": f"{formatted_start_date}T00:00:00", "aggregateHours": "1",
                   "location": "Windsor,CT,USA",
                   "endDateTime": f"{formatted_end_date}T{formatted_end_hour}:00:00", "unitGroup": "us",
                   "dayStartTime": "6:00:00", "contentType": "json", "dayEndTime": "19:00:00", "shortColumnNames": "0"}

    headers = {
        "X-RapidAPI-Key": "915ad04937mshe41de0b3b8249ccp11a135jsneda28e1016e0",
        "X-RapidAPI-Host": "visual-crossing-weather.p.rapidapi.com"
    }

    return requests.request("GET", url, headers=headers, params=querystring).json()


def API_json_query(json_file):
    """ queries through json file and filter out unnecessary info

    :param json_file: unfiltered json file from API
    :return: dictionary of dictionary. {"day": {"hr": {"temp": temp, "weather": weather_conditions}}}
    """

    weather_record = {}

    for entry in json_file["locations"]["Windsor,CT,USA"]["values"]:  # loops through the JSON response to parse data

        if 6 <= int(entry['datetimeStr'][11:13]) <= 18:
            current_day = entry['datetimeStr'][:10]
            if current_day not in weather_record:
                weather_record[current_day] = {}

            current_hr = entry['datetimeStr'][11:13]
            if current_hr not in weather_record[current_day]:
                weather_record[current_day][current_hr] = {}

            weather_record[current_day][current_hr]["temp"] = entry["temp"]
            weather_record[current_day][current_hr]["weather"] = entry["conditions"]

    return weather_record


def solar_csv_query(file_location):
    """ queries through solar csv file and filter out inverter data & average out hourly data

    :param file_location: the location of the csv file of solar data
    :return: dictionary of dictionary. {"day": {"hr": energy_produced, "hr": energy_produced}}
    """

    solar_record = {}

    solar_csv = open(file_location, "r")

    # creates a list, each item is a new line "date+time, meter, inverter" skip header line
    solar_list = solar_csv.read().split("\n")[1:]

    # loops through each line in the csv, e.x. entry = ""2022-09-27 17:00:00",115.5392,115.9913"
    for entry in solar_list:
        entry = entry.split(",")  # creates a list ["2022-09-27 17:00:00", "115.5392", "115.9913"]
        entry[0] = entry[0].replace('"', "")

        if 6 <= int(entry[0][11:13]) <= 18:
            current_day = entry[0][:10]
            if current_day not in solar_record:
                solar_record[current_day] = {}

            current_hr = entry[0][11:13]
            if current_hr not in solar_record[current_day]:
                solar_record[current_day][current_hr] = 0

            solar_record[current_day][current_hr] += float(entry[1])

    solar_csv.close()

    return solar_record


def combine_records(weather_record, solar_record):
    """ combines the two dictionary into one uniformed dict for further analysis

    :param weather_record: filtered dict of weather data
    :param solar_record: filtered dict of solar data
    :return: combined_record: dictionary of dictionary of dictionary.
                {day: [{hr: {temp: temp, weather: weather, energy: energy}}, total energy of day]}
             stat_record: dictionary of list of energy values
                {weather condition: [# of occurrence, total energy produced under that condition]}
    """

    combined_record = {}
    stat_record = {}

    # joining weather_record and solar_record
    for date in weather_record:
        combined_record[date] = [{}, 0]
        for hour in weather_record[date]:
            if solar_record[date][hour]:
                combined_record[date][0][hour] = {"temp": weather_record[date][hour]["temp"],
                                                  "weather": weather_record[date][hour]["weather"],
                                                  "energy": solar_record[date][hour]}

            combined_record[date][1] += solar_record[date][hour]

    # query combined_record to create stat_record
    for date in combined_record:
        for hour in combined_record[date][0]:
            current_weather = combined_record[date][0][hour]["weather"]
            if current_weather not in stat_record:
                stat_record[current_weather] = [0, 0]

            stat_record[current_weather][0] += 1
            stat_record[current_weather][1] += combined_record[date][0][hour]["energy"]

    return combined_record, stat_record


def stat_query(stat_record):
    """ queries through stat_record dictionary to return equal-length lists

    :param stat_record: dictionary of weather conditions as key and [# of occurrences, total energy produced] as value
    :return: three lists:
             weather_condition = [condition1, condition2, condition3]
             condition_count = [condition1_#_occurrences, condition2_#_occurrences, condition3_#_occurrences]
             energy_produced = [condition1_total_energy, condition2_total_energy, condition3_total_energy]
    """

    weather_condition = list(stat_record.keys())
    condition_count = []
    energy_produced = []

    for entry in stat_record:
        condition_count.append(stat_record[entry][0])
        energy_produced.append(stat_record[entry][1] / stat_record[entry][0])  # avg energy produced during condition

    return weather_condition, condition_count, energy_produced


def scatter_graph_generate(combined_record, title, x_label, y_label):
    """ generates a scatter plot of energy produced over the course of a day with indicators of weather conditions

    :param combined_record: dictionary of dictionary of dictionary.
                {day: [{hr: {temp: temp, weather: weather, energy: energy}}, total energy of day]}
    :param title: title of the plot
    :param x_label: x_axis label
    :param y_label: y_axis label
    """

    time = []
    energy = []
    weather_record = []

    # https://stackoverflow.com/questions/41832613/python-input-validation-how-to-limit-user-input-to-a-specific-range-of-integers
    while True:
        try:
            day = int(
                input("Please input the day you want to be graphed (1 = today, 2 = yesterday, 3 = two days ago): "))
            if not (1 <= day <= 3):
                raise ValueError  # this will send it to the print message and back to the input option
            break
        except ValueError:
            print("Invalid input. Must be integer in the range of 1-3.\n")

    day_record = combined_record[list(combined_record.keys())[-day]]  # dictionaries of the last day, dicts are ordered

    # Dict of all realistically possible weather conditions in CT and their legend color
    colors = {"Blowing Or Drifting Snow": "cyan", "Clear": "blue", "Partially cloudy": "green",
              "Overcast": "darkorange", "Drizzle": "slateblue", "Heavy Drizzle": "blue",
              "Light Drizzle": "lightsteelblue", "Fog": "slategray", "Freezing Rain": "mediumpurple",
              "Freezing Fog": "indigo", "Tornado": "firebrick", "Hail Showers": "rebeccapurple",
              "Ice": "cornflowerblue", "Lighting Without Thunder": "thistle", "Mist": "lightcyan",
              "Precipitation In Vicinity": "lightsteelblue", "Rain": "royalblue", "Heavy Rain And Snow": "darkgreen",
              "Light Rain And Snow": "limegreen", "Rain Showers": "tan", "Heavy Rain": "chocolate",
              "Light Rain": "sandybrown", "Smoke Or Haze": "slategray", "Snow": "lightcoral",
              "Snow Showers": "olive", "Heavy Snow": "teal", "Light Snow": "aquamarine",
              "Squalls": "rosybrown", "Thunderstorm": "red", "Diamond Dust": "beige",
              "Hail": "orchid", "Rain, Overcast": "indianred"}

    legend_colors = {}

    for entry in day_record[0]:
        time.append(f"{entry}:00:00")
        energy.append(day_record[0][entry]["energy"])
        weather_record.append(day_record[0][entry]["weather"])

    for condition in weather_record:
        legend_colors[condition] = colors[condition]

    # Sets up a dict for pandas DataFrame
    plot_dict = {
        "time": time,
        "energy": energy,
        "condition": weather_record
    }

    df = pd.DataFrame(plot_dict)

    fig, ax = plt.subplots()

    ax.plot(df["time"], df["energy"], ls="--", c="silver")
    ax.scatter(df["time"], df["energy"], c=df["condition"].map(legend_colors), label=legend_colors)

    # https://stackoverflow.com/questions/31303912/matplotlib-pyplot-scatterplot-legend-from-color-dictionary
    markers = [plt.Line2D([0, 0], [0, 0], color=color, marker='o', linestyle='') for color in legend_colors.values()]
    plt.legend(markers, legend_colors.keys(), numpoints=1)

    fig.canvas.manager.set_window_title(title + f" ({list(combined_record.keys())[-day]})")
    plt.title(title + f" ({list(combined_record.keys())[-day]})")
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    plt.show()


def bar_graph_generate(x, y, title, x_label, y_label):
    """ creates a bar graph of the total energy produced per weather conditions

    :param x: list of weather conditions
    :param y: list of total energy produced per weather conditions
    :param title: title of the plot
    :param x_label: x_axis label
    :param y_label: y_axis label
    """

    fig = plt.figure(figsize=(10, 5))

    plt.bar(x, y, color="maroon", width=0.4)

    fig.canvas.manager.set_window_title(title)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    plt.show()


def main():
    solar_file_location = csv_grabber()
    response = API_grabber(solar_file_location)

    weather_record = API_json_query(response)
    solar_record = solar_csv_query(solar_file_location)

    combined_record, stat_record = combine_records(weather_record, solar_record)

    weather_condition, condition_count, energy_produced = stat_query(stat_record)

    scatter_graph_generate(combined_record, "Condition During Day vs Energy Produced", "Time of Day (hh:mm:ss)",
                           "Energy Produced in KiloWatts")

    bar_graph_generate(weather_condition, energy_produced,
                       f"Energy Produced vs Different Weather Conditions ({list(combined_record.keys())[0]} to "
                       f"{list(combined_record.keys())[-1]})", "Weather Condition", "Energy Produced in KiloWatts")


if __name__ == "__main__":
    main()
