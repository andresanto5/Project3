import argparse
import logging
import os
import sys
from db_util import dbmanip as db
import util.loggerinitializer as utl

# Objeto log de inicialização
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
utl.initialize_logger(os.getcwd(), logger)

#Função principal com argparse
def main ():
	parser = argparse.ArgumentParser(description="A Tool manipulate a sqlite DB")

	subparsers = parser.add_subparsers(title='actions',
									   description='valid actions',
									   help='Use sqlite-project3.py {acrion} -h for help  with each action',
									   dest='command'
									   )
# Criando indice
	parser_index = subparsers.add_parser('createdb', help="Create database and tables for project3")

	parser_index.add_argument("--db", dest='db', default=None, action="store", help="The DB name",
					   required=True)
# Criando argumento para inserção de dados
	parser_insert = subparsers.add_parser('insert', help='Insert data on tables')

	parser_insert.add_argument("--file", default=None, action="store", help="TSV file with the data to be inserted",
					    required=True)

	parser_insert.add_argument("--db", default=None, action="store", help="The DB name",
						 required=True)

# Criando o argumento para atualização dos dados
	parser_update = subparsers.add_parser('update', help='Update a field in a db')

	parser_update.add_argument("--db", default=None, action="store", help="The DB name",
						 required=True)

	parser_update.add_argument("--assay", default=None, action="store", help="Assay of the table",
						 required=False)

	parser_update.add_argument("--new_assay", default=None, action="store", help="New_assay of the table",
						 required=False)

	parser_update.add_argument("--donor", default=None, action="store", help="Donor of the table",
						 required=False)

	parser_update.add_argument("--new_donor", default=None, action="store", help="New_donor of the table",
						 required=False)



# Criando o argumento para selecionar itens da tabela
	parser_select = subparsers.add_parser('select', help='Select fields from the db')

	parser_select.add_argument("--db", action="store", help="The DB name",
						 required=True)

	parser_select.add_argument("--cell_type", action="store_true", help="Select all cell_type", default=False, required=False)

	parser_select.add_argument("--assay_track", action="store", help="Select all specific assay_category with tracks", default=False, required=False)

	parser_select.add_argument("--track_name", action="store", help="Select all specific track_name using asay_track_name with key", default=False, required=False)

	parser_select.add_argument("--assay", action="store", help="Select all cell_type using assay with key", default=False, required=False)


# Criando argumento para deletar na tabela
	parser_delete = subparsers.add_parser('delete', help='delete rows from the db')

	parser_delete.add_argument("--track_name", default=False, action="store", help="Delete rows where track_name appear",
						 required=False)

	parser_delete.add_argument("--db", default=False, action="store", help="The DB name",
						 required=True)

# Chamando as funções do args
	args = parser.parse_args()


# Conectar o DB + args + logger
	conn = db.connect_db(args.db, logger)

	if args.command == "createdb":

		db.create_table(conn, logger)

	elif args.command == "insert":
		list_of_data = []

		with open(args.file, 'r') as f:
			for line in f:

				#resetar o dicionário
				line_dict = dict()

				#Pular linhas vazias
				if not line.strip():
					continue
				if line.startswith(",,"):
					continue

				
				#Split line
				values = line.strip().split(',')

				# colocar cada campo no dicionário. As colunas da tabela
				line_dict['cell_type_category'] = values[0]
				line_dict['cell_type'] = values[1]
				line_dict['cell_type_track_name'] = values[2]
				line_dict['cell_type_short'] = values[3]
				line_dict['assay_category'] = values[4]
				line_dict['assay'] = values[5]
				line_dict['assay_track_name'] = values[6]
				line_dict['assay_short'] = values[7]
				line_dict['donor'] = values[8]
				line_dict['time_point'] = values[9]
				line_dict['view'] = values[10]
				line_dict['track_name'] = values[11]
				line_dict['track_type'] = values[12]
				line_dict['track_density'] = values[13]
				line_dict['provider_institution'] = values[14]
				line_dict['source_server'] = values[15]
				line_dict['source_path_to_file'] = values[16]
				line_dict['server'] = values[17]
				line_dict['path_to_file'] = values[18]
				line_dict['new_file_name'] = values[19]

				#adicionar na lista de diretório
				list_of_data.append(line_dict)

		db.insert_data(conn, list_of_data, logger)


	elif args.command == "update":
		db.update_assay(conn, args.assay, args.new_assay, logger) 

		db.update_donor(conn, args.donor, args.new_donor, logger)



	elif args.command == "select" and args.cell_type is not False:
		all_cell_type = db.select_cell_type(conn, logger)

		for cell in all_cell_type:
			print(cell[0])


	elif args.command == "select" and args.assay_track is not False:
		all_assay_track = db.select_assay_track(conn, args.assay_track, logger)

		print("\n| Track name\t| Track type\t| Track density")
		for track in all_assay_track:
			print("|","\t| ".join(track))


	elif args.command == "select" and args.track_name is not False:
		all_track_name = db.select_track_name(conn, args.track_name, logger)

		print("\n| Assay track name\t| Track anme")
		for name in all_track_name:
			print("|","\t| ".join(name))

	elif args.command == "select" and args.assay is not False:
		all_assay = db.select_assay(conn, args.assay, logger)

		print("\n| Assay\t| Cell type")
		for name in all_assay:
			print("|","\t| ".join(name))



	elif args.command == "delete":
		db.delete_track_name(conn, args.track_name, logger)
		

if __name__ == '__main__':
	main()