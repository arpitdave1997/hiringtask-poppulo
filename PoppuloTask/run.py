import os
import pandas
import xml.etree.ElementTree as ElementTree
from datetime import datetime
from glob import glob
from datetime import date

DIRECTORY_PATH = f"{os.path.dirname(__file__)}"
SUBDIRECTORY_PATH = f"{os.path.dirname(__file__)}/{str(date.today())}"
LOGFILE_PATH = f'{DIRECTORY_PATH}/log.txt'

class LoggingOperations():

        def initialize_logs(self):
                if not os.path.exists(LOGFILE_PATH):
                        with open(LOGFILE_PATH, 'w') as file:
                                file.write('')
                return
        
        @staticmethod
        def create_log(logEvent: str, logMessage: str):
                message = f'[{datetime.now().strftime("%Y-%m-%d %H:%M")} : {logEvent}] \n{logMessage}\n\n'
                with open(LOGFILE_PATH, 'a') as file:
                        file.write(message)

                return

class DirectoryOperations():

        def initialize_subdirectory(self):
                if not os.path.exists(SUBDIRECTORY_PATH):
                        os.makedirs(SUBDIRECTORY_PATH)
                return
        
        def get_file_names(self):
                xlsxFiles = glob(os.path.join(DIRECTORY_PATH, '*.xlsx'))
                return xlsxFiles

class PandasOperations():

        def read_excel(self, filePath: str):
                try:
                        pandasDataFrame = pandas.read_excel(filePath)
                        return True, pandasDataFrame
                except Exception as e:
                        LoggingOperations.create_log('READ_EXCEL', e)
                        return False, None

        def read_csv(self, filePath: str):
                try:
                        pandasDataFrame = pandas.read_csv(filePath)
                        return True, pandasDataFrame
                except Exception as e:
                        LoggingOperations.create_log('READ_CSV', e)
                        return False, None
                
        def get_column_values(self, rawData: pandas.DataFrame):
                return rawData.columns.values        

class DataOperations():

        def set_data_headers(self, rawData: pandas.DataFrame):
                filePath = f'{SUBDIRECTORY_PATH}/headers.txt'
                pandasOperations = PandasOperations()

                headers = pandasOperations.get_column_values(rawData)
                try:
                        with open(filePath, 'w+') as file:
                                file.write(', '.join(headers))
                        return True
                except Exception as e:
                        LoggingOperations.create_log('SET_DATA_HEADERS', e)
                        return False

        def set_summary_report(self, rawData: pandas.DataFrame, columns: list[str]):
                reportFilePath = f'{SUBDIRECTORY_PATH}/csv_report.csv'
                reportColumns = ['Percentage Filled', 'Percentage Not Filled', 'Total Values', 'Distinct Values', 'Values Not Filled']
                reportData = []

                try:
                        for column in columns:
                                totalValues = rawData.size
                                totalUnfilled = rawData[column].isna().sum()
                                distinctValues = rawData[column].nunique()
                                percentUnfilled = (totalUnfilled / totalValues) * 100
                                percentFilled = 100 - percentUnfilled
                                
                                reportData.append([int(percentFilled), int(percentUnfilled), totalValues, distinctValues, totalUnfilled])

                        reportDataframe = pandas.DataFrame(data = reportData)
                        reportDataframe.columns = reportColumns
                        reportDataframe.index = columns

                        reportDataframe.to_csv(reportFilePath)
                        return True
                except Exception as e:
                        LoggingOperations.create_log('SET_SUMMARY_REPORT', e)
                        return False
                
        def set_department_report(self, rawData: pandas.DataFrame):
                try:
                        allDepartments = rawData['Department'].unique()

                        for department in allDepartments:
                                departmentPath = f'{SUBDIRECTORY_PATH}/{department}.csv'
                                departmentData: pandas.DataFrame = rawData[rawData['Department'] == department]
                                departmentData.to_csv(departmentPath, index = False)
                        return True
                except Exception as e:
                        LoggingOperations.create_log('SET_DEPARTMENT_REPORT', e)
                        return False

        def set_nocountries_report(self, rawData: pandas.DataFrame):
                noCountriesPath = f'{SUBDIRECTORY_PATH}/no_countires.csv'
                
                try:
                        noCountriesData: pandas.DataFrame = rawData.drop(columns = ['Country'])
                        noCountriesData.to_csv(noCountriesPath, index = False)
                        return True
                except Exception as e:
                        LoggingOperations.create_log('SET_NOCOUNTRIES_REPORT', e)
                        return False

        def set_master_XML(self, rawData: pandas.DataFrame, columns: list[str]):
                masterXMLPath = f'{SUBDIRECTORY_PATH}/master.xml'

                try:
                        root = ElementTree.Element('subscriber_import_job')

                        ElementTree.SubElement(root, 'accept_terms').text = 'true'
                        ElementTree.SubElement(root, 'reactivate_api_removed').text = 'false'
                        ElementTree.SubElement(root, 'reactivate_admin_removed').text = 'true'
                        ElementTree.SubElement(root, 'reactivate_bounced_removed').text = 'false'

                        tags = ElementTree.SubElement(root, 'tags')
                        ElementTree.SubElement(tags, 'tag', name = "Employee Data")

                        subscriberData = ElementTree.SubElement(root, 'subscriber_data')
                        ElementTree.SubElement(subscriberData, 'columns').text = ','.join(columns)
                        ElementTree.SubElement(subscriberData, 'skip_first_line').text = 'true'
                        ElementTree.SubElement(subscriberData, 'field_separator').text = 'comma'
                        ElementTree.SubElement(subscriberData, 'data').text = rawData.to_csv(index = False, header = True)
                        
                        formattedXML = ElementTree.ElementTree(root)
                        formattedXML.write(masterXMLPath)
                        return True
                
                except Exception as e:
                        LoggingOperations.create_log('SET_MASTER_XML', e)
                        return False

        def set_child_XML(self, fileNames: list[str], columns: list[str]):
                try:
                        for fileName in fileNames:
                                filePath = f'{SUBDIRECTORY_PATH}/{fileName}.csv'
                                childXMLPath = f'{SUBDIRECTORY_PATH}/{fileName}.xml'
                                departmentData = pandas.read_csv(filePath)
                                root = ElementTree.Element('subscriber_import_job')

                                ElementTree.SubElement(root, 'accept_terms').text = 'true'
                                ElementTree.SubElement(root, 'reactivate_api_removed').text = 'false'
                                ElementTree.SubElement(root, 'reactivate_admin_removed').text = 'true'
                                ElementTree.SubElement(root, 'reactivate_bounced_removed').text = 'false'

                                tags = ElementTree.SubElement(root, 'tags')
                                ElementTree.SubElement(tags, 'tag', name = "Employee Data")

                                subscriberData = ElementTree.SubElement(root, 'subscriber_data')
                                ElementTree.SubElement(subscriberData, 'columns').text = ','.join(columns)
                                ElementTree.SubElement(subscriberData, 'skip_first_line').text = 'true'
                                ElementTree.SubElement(subscriberData, 'field_separator').text = 'comma'
                                ElementTree.SubElement(subscriberData, 'data').text = departmentData.to_csv(index = False, header = True)
                                
                                formattedXML = ElementTree.ElementTree(root)
                                formattedXML.write(childXMLPath)

                        return True
                except Exception as e:
                        LoggingOperations.create_log('SET_CHILD_XML', e)
                        return False

        def process_file(self):
                directoryOperations = DirectoryOperations()
                pandasOperations = PandasOperations()

                loggingOperations = LoggingOperations()
                loggingOperations.initialize_logs()

                directoryOperations.initialize_subdirectory()
                files = directoryOperations.get_file_names()
                if files == []:
                        return
                
                status, rawData = pandasOperations.read_excel(files[0])
                if not status:
                        return
                else:
                        columns = pandasOperations.get_column_values(rawData)
                
                status = self.set_data_headers(rawData)
                if not status:
                        return

                status = self.set_summary_report(rawData, columns)
                if not status:
                        return

                status = self.set_department_report(rawData)
                if not status:
                        return

                status = self.set_nocountries_report(rawData)
                if not status:
                        return

                status = self.set_master_XML(rawData, columns)
                if not status:
                        return

                departments = rawData['Department'].unique()
                status = self.set_child_XML(departments, columns)
                if not status:
                        return

                return

if __name__ == "__main__":
        dataOperations = DataOperations()
        dataOperations.process_file()